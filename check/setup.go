package check

import (
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/optiopay/kafka/proto"
	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

const MainLockPath = "main_lock"

func (check *HealthCheck) reconnect(stop <-chan struct{}, wg *sync.WaitGroup) error {
	if err := check.closeConnection(false); err != nil {
		log.Warnf("failed to close connection: %s", err)
	}
	return check.connect(false, stop, wg)
}

func (check *HealthCheck) connect(firstConnection bool, stop <-chan struct{}, wg *sync.WaitGroup) error {
	wg.Add(1)
	defer wg.Done()
	var createHealthTopicIfMissing = firstConnection
	var createReplicationTopicIfMissing = firstConnection
	ticker := time.NewTicker(check.config.retryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := check.tryConnectOnce(&createHealthTopicIfMissing, &createReplicationTopicIfMissing); err == nil {
				return nil
			}
		case <-stop:
			return errors.New("connect was asked to stop")
		}
	}
}

func (check *HealthCheck) tryConnectOnce(createBrokerTopic, createReplicationTopic *bool) error {
	pauseTime := check.config.retryInterval
	// connect to kafka cluster
	connectString := []string{fmt.Sprintf("%s:%d", check.config.brokerHost, check.config.brokerPort)}
	err := check.broker.Dial(connectString, check.brokerConfig())
	if err != nil {
		log.Infof("unable to connect to broker, retrying in %s (%s)", pauseTime.String(), err)
		return err
	}

	metadata, err := check.broker.Metadata()
	if err != nil {
		return errors.Wrap(err, "failure retrieving metadata")
	}

	check.partitionID, err = check.findPartitionID(check.config.topicName, true, createBrokerTopic, metadata)
	if err != nil {
		log.Infof("%s retrying in %s", err.Error(), pauseTime)
		check.broker.Close()
		return err
	}

	check.replicationPartitionID, err = check.findPartitionID(check.config.replicationTopicName, false, createReplicationTopic, metadata)
	if err != nil {
		log.Infof("%s retrying in %s", err.Error(), pauseTime)
		check.broker.Close()
		return err
	}

	consumer, err := check.broker.Consumer(check.consumerConfig())
	if err != nil {
		log.Infof("unable to create consumer, retrying in %s: %s", pauseTime.String(), err)
		check.broker.Close()
		return err
	}

	producer := check.broker.Producer(check.producerConfig())

	check.consumer = consumer
	check.producer = producer
	return nil
}

func (check *HealthCheck) findPartitionID(topicName string, forHealthCheck bool, createIfMissing *bool, metadata *proto.MetadataResp) (int32, error) {
	brokerID := int32(check.config.brokerID)

	if !brokerExists(brokerID, metadata) {
		return 0, fmt.Errorf("unable to find broker %d in metadata", brokerID)
	}

	topic, ok := findTopic(topicName, metadata)

	if ok {
		for _, partition := range topic.Partitions {
			if forHealthCheck && partition.Leader != brokerID {
				continue
			}
			if !contains(partition.Replicas, brokerID) {
				continue
			}

			log.Infof(`found partition id %d for broker %d in topic "%s"`, partition.ID, brokerID, topicName)
			return partition.ID, nil
		}
	}

	if *createIfMissing {
		err := check.createTopic(topicName, forHealthCheck)
		if err != nil {
			return 0, errors.Wrapf(err, `unable to create topic "%s"`, topicName)
		}
		log.Infof(`topic "%s" created`, topicName)
		*createIfMissing = false
		return 0, errors.New("topic created, try again")
	}

	if ok {
		return 0, fmt.Errorf(`unable to find broker's parition in topic "%s" in metadata`, topicName)
	} else {
		return 0, fmt.Errorf(`unable to find broker's topic "%s" in metadata`, topicName)
	}
}

func findTopic(name string, metadata *proto.MetadataResp) (*proto.MetadataRespTopic, bool) {
	for _, topic := range metadata.Topics {
		if topic.Name == name {
			return &topic, true
		}
	}

	return nil, false
}

func brokerExists(brokerID int32, metadata *proto.MetadataResp) bool {
	for _, broker := range metadata.Brokers {
		if broker.NodeID == brokerID {
			return true
		}
	}
	return false
}

func zookeeperEnsembleAndChroot(connectString string) (ensemble []string, chroot string) {
	result := strings.Split(connectString, "/")
	switch len(result) {
	case 1:
		ensemble = strings.Split(result[0], ",")
		chroot = ""
	default:
		ensemble = strings.Split(result[0], ",")
		chroot = "/" + strings.Join(result[1:], "/")
		if strings.HasSuffix(chroot, "/") {
			chroot = chroot[:len(chroot)-1]
		}
	}
	return
}

func (check *HealthCheck) createTopic(name string, forHealthCheck bool) (err error) {
	log.Printf("connecting to ZooKeeper ensemble %s", check.config.zookeeperConnect)
	connectString, chroot := zookeeperEnsembleAndChroot(check.config.zookeeperConnect)
	zkConn := check.zookeeper

	if _, err = zkConn.Connect(connectString, 10*time.Second); err != nil {
		return
	}
	defer zkConn.Close()

	lockPath := path.Join(chroot, "healthcheck", MainLockPath)
	log.Infof("taking lock for creating topic %s", name)
	if err := zkConn.Lock(lockPath); err != nil {
		return err
	}
	log.Infof("lock acquired for creating topic %s", name)
	defer zkConn.Unlock(lockPath)

	topicPath := chroot + "/config/topics/" + name

	exists := false
	if !forHealthCheck {
		exists, _, err = zkConn.Exists(topicPath)
		if err != nil {
			return
		}
	}

	brokerID := int32(check.config.brokerID)

	if !exists {
		topicConfig := `{"version":1,"config":{"delete.retention.ms":"10000",` +
			`"cleanup.policy":"delete","compression.type":"uncompressed",` +
			`"min.insync.replicas":"1"}}`
		log.Infof(`creating topic "%s" configuration node`, name)

		if err = createZkNode(zkConn, topicPath, topicConfig, forHealthCheck); err != nil {
			return
		}

		partitionAssignment := fmt.Sprintf(`{"version":1,"partitions":{"0":[%d]}}`, brokerID)
		log.Infof(`creating topic "%s" partition assignment node`, name)

		if err = createZkNode(zkConn, chroot+"/brokers/topics/"+name, partitionAssignment, forHealthCheck); err != nil {
			return
		}
	}

	if !forHealthCheck {
		err = maybeExpandReplicationTopic(zkConn, brokerID, check.replicationPartitionID, name, chroot)
	}

	return

}

func maybeExpandReplicationTopic(zk ZkConnection, brokerID, partitionID int32, topicName, chroot string) error {
	topic := ZkTopic{Name: topicName}
	err := zkPartitions(&topic, zk, topicName, chroot)
	if err != nil {
		return errors.Wrap(err, "unable to determine if replication topic should be expanded")
	}

	replicas, ok := topic.Partitions[partitionID]
	if !ok {
		return fmt.Errorf(`cannot find partition with ID %d in topic "%s"`, partitionID, topicName)
	}

	if !contains(replicas, brokerID) {
		log.Info("expanding replication check topic to include broker ", brokerID)
		replicas = append(replicas, brokerID)

		return reassignPartition(zk, partitionID, replicas, topicName, chroot)
	}
	return nil
}

func reassignPartition(zk ZkConnection, partitionID int32, replicas []int32, topicName, chroot string) (err error) {

	log.Infof("reassigning partition %d on %#v for topic %s", partitionID, replicas, topicName)

	repeat := true
	for repeat {
		time.Sleep(1 * time.Second)
		exists, _, rp_err := zk.Exists(chroot + "/admin/reassign_partitions")
		if rp_err != nil {
			log.Warn("error while checking if reassign_partitions node exists", err)
		}
		repeat = exists || err != nil
	}

	var replicasStr []string
	for _, ID := range replicas {
		replicasStr = append(replicasStr, fmt.Sprintf("%d", ID))
	}

	reassign := fmt.Sprintf(`{"version":1,"partitions":[{"topic":"%s","partition":%d,"replicas":[%s]}]}`,
		topicName, partitionID, strings.Join(replicasStr, ","))

	repeat = true
	for repeat {
		log.Info("creating reassign partition node")
		err = createZkNode(zk, chroot+"/admin/reassign_partitions", reassign, true)
		if err != nil {
			log.Warn("error while creating reassignment node", err)
		}
		repeat = err != nil
	}

	return
}

func createZkNode(zookeeper ZkConnection, path string, content string, failIfExists bool) error {
	nodeExists, _, err := zookeeper.Exists(path)

	if err != nil {
		return err
	}

	if nodeExists {
		if failIfExists {
			return fmt.Errorf("node %s cannot be created, exists already", path)
		}
		return nil
	}

	log.Infof("creating node", path)
	flags := int32(0) // permanent node.
	acl := zk.WorldACL(zk.PermAll)
	_, err = zookeeper.Create(path, []byte(content), flags, acl)
	return err
}

func (check *HealthCheck) closeConnection(deleteTopicIfPresent bool) error {
	log.Debug("closing connection to the cluster")
	defer check.broker.Close()
	if deleteTopicIfPresent {
		log.Infof("connecting to ZooKeeper ensemble %s", check.config.zookeeperConnect)
		connectString, chroot := zookeeperEnsembleAndChroot(check.config.zookeeperConnect)

		zkConn := check.zookeeper
		_, err := zkConn.Connect(connectString, 10*time.Second)
		if err != nil {
			return err
		}
		defer zkConn.Close()

		// taking clusterwide lock here
		lockPath := path.Join(chroot, "healthcheck", MainLockPath)
		log.Info("taking lock to close the connection")
		err = zkConn.Lock(lockPath)
		if err != nil {
			return fmt.Errorf("error while taking cluster lock: %w", err)
		}
		log.Info("lock acquired to close the connection")
		if err = check.deleteTopic(zkConn, chroot, check.config.topicName, check.partitionID); err != nil {
			log.Errorf("error while deleting topic %s: %s", check.config.topicName, err)
		}
		if err = check.deleteTopic(zkConn, chroot, check.config.replicationTopicName, check.replicationPartitionID); err != nil {
			log.Errorf("error while deleting topic %s: %s", check.config.replicationTopicName, err)
		}
		err = zkConn.Unlock(lockPath)
		if err != nil {
			return fmt.Errorf("error while releasing cluster lock: %w", err)
		}
	}
	return nil
}

func (check *HealthCheck) deleteTopic(zkConn ZkConnection, chroot, name string, partitionID int32) error {
	log.Infof("removing from topic %s", name)
	topic := ZkTopic{Name: name}
	err := zkPartitions(&topic, zkConn, name, chroot)
	if err != nil {
		return err
	}

	replicas, ok := topic.Partitions[partitionID]
	if !ok {
		return fmt.Errorf(`Cannot find partition with ID %d in topic "%s"`, partitionID, name)
	}

	brokerID := int32(check.config.brokerID)
	if len(replicas) > 1 {
		log.Info("Shrinking replication check topic to exclude broker ", brokerID)
		replicas = delAll(replicas, brokerID)
		return reassignPartition(zkConn, partitionID, replicas, name, chroot)
	}

	log.Infof("topic %s has only one replica, deleting it", name)

	delTopicPath := chroot + "/admin/delete_topics/" + name

	err = createZkNode(check.zookeeper, delTopicPath, "", true)
	if err != nil {
		return err
	}
	return check.waitForTopicDeletion(delTopicPath)
}

func (check *HealthCheck) waitForTopicDeletion(topicPath string) error {
	for {
		log.Infof("checking zookeeper for deletion of node \"%s\"", topicPath)
		exists, _, err := check.zookeeper.Exists(topicPath)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		time.Sleep(check.config.retryInterval)
	}
}
