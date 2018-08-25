package check

import (
	"fmt"
	"strings"
	"time"

	"github.com/optiopay/kafka/proto"
	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

func (check *HealthCheck) connect(firstConnection bool, stop <-chan struct{}) error {
	var createHealthTopicIfMissing = firstConnection
	var createReplicationTopicIfMissing = firstConnection
	var MaxRetries = 5
	ticker := time.NewTicker(check.config.retryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := check.tryConnectOnce(&createHealthTopicIfMissing, &createReplicationTopicIfMissing); err == nil {
				return nil
			}else{
				MaxRetries = MaxRetries - 1
				if MaxRetries == 0{
					return errors.New("cannot connect")
				}
			}
		case <-stop:
			log.Println("connection was asked to stop")
			return errors.New("connect was asked to stop")
		}
	}
}

func (check *HealthCheck) tryConnectOnce(createBrokerTopic, createReplicationTopic *bool) error {
	pauseTime := check.config.retryInterval
	// connect to kafka cluster
	connectString := []string{fmt.Sprintf("%s:%d", check.config.brokerHost, check.config.brokerPort)}

	log.Println("connectString " + connectString[0])
	err := check.broker.Dial(connectString, check.brokerConfig())
	if err != nil {
		log.Printf("unable to connect to broker, retrying in %s (%s)", pauseTime.String(), err)
		return err
	}

	metadata, err := check.broker.Metadata()
	if err != nil {
		return errors.Wrap(err, "failure retrieving metadata")
	}

	check.partitionID, err = check.findPartitionID(check.config.topicName, true, createBrokerTopic, metadata)
	if err != nil {
		log.Printf("%s retrying in %s", err.Error(), pauseTime)
		check.broker.Close()
		return err
	}

	check.replicationPartitionID, err = check.findPartitionID(check.config.replicationTopicName, false, createReplicationTopic, metadata)
	if err != nil {
		log.Printf("%s retrying in %s", err.Error(), pauseTime)
		check.broker.Close()
		return err
	}

	consumer, err := check.broker.Consumer(check.consumerConfig())
	if err != nil {
		log.Printf("unable to create consumer, retrying in %s: %s", pauseTime.String(), err)
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

			log.Printf(`found partition id %d for broker %d in topic "%s"`, partition.ID, brokerID, topicName)
			return partition.ID, nil
		}
	}

	if *createIfMissing {
		err := check.createTopic(topicName, forHealthCheck)
		if err != nil {
			return 0, errors.Wrapf(err, `unable to create topic "%s"`, topicName)
		}
		log.Printf(`topic "%s" created`, topicName)
		*createIfMissing = false
		return 0, errors.New("topic created, try again")
	}

	if ok {
		return 0, fmt.Errorf(`Unable to find broker's parition in topic "%s" in metadata`, topicName)
	} else {
		return 0, fmt.Errorf(`Unable to find broker's topic "%s" in metadata`, topicName)
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
			`"cleanup.policy":"delete","compression.type":"uncompressed"}}`
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
		return errors.Wrap(err, "Unable to determine if replication topic should be expanded")
	}

	replicas, ok := topic.Partitions[partitionID]
	if !ok {
		return fmt.Errorf(`Cannot find partition with ID %d in topic "%s"`, partitionID, topicName)
	}

	if !contains(replicas, brokerID) {
		log.Info("Expanding replication check topic to include broker ", brokerID)
		replicas = append(replicas, brokerID)

		return reassignPartition(zk, partitionID, replicas, topicName, chroot)
	}
	return nil
}

func reassignPartition(zk ZkConnection, partitionID int32, replicas []int32, topicName, chroot string) (err error) {

	repeat := true
	for repeat {
		time.Sleep(1 * time.Second)
		exists, _, rp_err := zk.Exists(chroot + "/admin/reassign_partitions")
		if rp_err != nil {
			log.Warn("Error while checking if reassign_partitions node exists", err)
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
		log.Info("Creating reassign partition node")
		err = createZkNode(zk, chroot+"/admin/reassign_partitions", reassign, true)
		if err != nil {
			log.Warn("Error while creating reassignment node", err)
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

	log.Println("creating node", path)
	flags := int32(0) // permanent node.
	acl := zk.WorldACL(zk.PermAll)
	_, err = zookeeper.Create(path, []byte(content), flags, acl)
	return err
}

func (check *HealthCheck) closeConnection(deleteTopicIfPresent bool) {
	if deleteTopicIfPresent {
		log.Infof("connecting to ZooKeeper ensemble %s", check.config.zookeeperConnect)
		connectString, chroot := zookeeperEnsembleAndChroot(check.config.zookeeperConnect)

		zkConn := check.zookeeper
		_, err := zkConn.Connect(connectString, 10*time.Second)
		if err != nil {
			return
		}
		defer zkConn.Close()

		check.deleteTopic(zkConn, chroot, check.config.topicName, check.partitionID)
		check.deleteTopic(zkConn, chroot, check.config.replicationTopicName, check.replicationPartitionID)
	}
	check.broker.Close()
}

func (check *HealthCheck) deleteTopic(zkConn ZkConnection, chroot, name string, partitionID int32) error {
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

	delTopicPath := chroot + "/admin/delete_topics/" + name

	err = createZkNode(check.zookeeper, delTopicPath, "", true)
	if err != nil {
		return err
	}
	return check.waitForTopicDeletion(delTopicPath)
}

func (check *HealthCheck) waitForTopicDeletion(topicPath string) error {
	for {
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

func (check *HealthCheck) reconnect(stop <-chan struct{}) error {
	check.closeConnection(false)
	return check.connect(false, stop)
}
