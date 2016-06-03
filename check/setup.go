package check

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/optiopay/kafka/proto"
	"github.com/samuel/go-zookeeper/zk"
)

func (check *HealthCheck) connect(firstConnection bool, stop <-chan struct{}) error {
	var createIfMissing = firstConnection
	ticker := time.NewTicker(check.config.retryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := check.tryConnectOnce(&createIfMissing); err == nil {
				return nil
			}
		case <-stop:
			return errors.New("connect was asked to stop")
		}
	}
}

func (check *HealthCheck) tryConnectOnce(createIfMissing *bool) error {
	pauseTime := check.config.retryInterval
	// connect to kafka cluster
	connectString := []string{fmt.Sprintf("localhost:%d", check.config.brokerPort)}
	err := check.broker.Dial(connectString, check.brokerConfig())
	if err != nil {
		log.Printf("unable to connect to broker, retrying in %s (%s)", pauseTime.String(), err)
		return err
	}

	check.partitionID, err = check.getBrokerPartitionID(createIfMissing)
	if err != nil {
		log.Printf("%s retrying in %s", err.Error(), pauseTime)
		check.broker.Close()
		return err
	}

	consumer, err := check.broker.Consumer(check.consumerConfig())
	if err != nil {
		log.Printf("unable to create consumer, retrying in %s (%s)", pauseTime.String(), err)
		check.broker.Close()
		return err
		return err
	}

	producer := check.broker.Producer(check.producerConfig())

	check.consumer = consumer
	check.producer = producer
	return nil
}

func (check *HealthCheck) getBrokerPartitionID(createIfMissing *bool) (int32, error) {
	brokerID := int32(check.config.brokerID)

	metadata, err := check.broker.Metadata()
	if err != nil {
		return 0, fmt.Errorf("failure retrieving metadata: %s", err.Error())
	}

	if !check.brokerExists(metadata) {
		return 0, fmt.Errorf("unable to find broker %d in metadata", brokerID)
	}

	for _, topic := range metadata.Topics {
		if topic.Name != check.config.topicName {
			continue
		}

		for _, partition := range topic.Partitions {
			if partition.Leader != brokerID {
				continue
			}
			log.Printf("found partition id %d for broker %d", partition.ID, brokerID)
			return partition.ID, nil
		}
	}

	if *createIfMissing {
		err = check.createHealthCheckTopic()
		if err != nil {
			return 0, fmt.Errorf("unable to create health check topic \"%s\": %s", check.config.topicName, err)
		}
		log.Printf("health check topic \"%s\" created for broker %d", check.config.topicName, brokerID)
		*createIfMissing = false
		return 0, errors.New("health check topic created, try again")
	}
	return 0, fmt.Errorf("unable to find topic and parition for broker %d in metadata", brokerID)
}

func (check *HealthCheck) brokerExists(metadata *proto.MetadataResp) bool {
	brokerID := int32(check.config.brokerID)
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

func (check *HealthCheck) createHealthCheckTopic() error {
	log.Printf("connecting to ZooKeeper ensemble %s", check.config.zookeeperConnect)
	connectString, chroot := zookeeperEnsembleAndChroot(check.config.zookeeperConnect)
	_, err := check.zookeeper.Connect(connectString, 10*time.Second)
	if err != nil {
		return err
	}
	defer check.zookeeper.Close()

	topicConfig := `{"version":1,"config":{"delete.retention.ms":"10000","cleanup.policy":"delete"}}`
	log.Println("creating topic", check.config.topicName, "configuration node")
	err = createZkNode(check.zookeeper, chroot+"/config/topics/"+check.config.topicName, topicConfig)
	if err != nil {
		return err
	}

	brokerID := int32(check.config.brokerID)
	partitionAssignment := `{"version":1,"partitions":{"0":[` + fmt.Sprintf("%d", brokerID) + `]}}`
	log.Println("creating topic", check.config.topicName, "partition assignment node")
	err = createZkNode(check.zookeeper, chroot+"/brokers/topics/"+check.config.topicName, partitionAssignment)
	return err
}

func createZkNode(zookeeper ZkConnection, path string, content string) error {
	nodeExists, _, err := zookeeper.Exists(path)

	if err != nil {
		return err
	}

	if nodeExists {
		return fmt.Errorf("node %s cannot be created, exists already", path)
	}

	log.Println("creating node ", path)
	flags := int32(0) // permanent node.
	acl := zk.WorldACL(zk.PermAll)
	_, err = zookeeper.Create(path, []byte(content), flags, acl)
	return err
}

func (check *HealthCheck) close(deleteTopicIfPresent bool) {
	if deleteTopicIfPresent {
		check.deleteHealthCheckTopic()
	}
	check.broker.Close()
}

func (check *HealthCheck) deleteHealthCheckTopic() error {
	log.Printf("connecting to ZooKeeper ensemble %s", check.config.zookeeperConnect)
	connectString, chroot := zookeeperEnsembleAndChroot(check.config.zookeeperConnect)
	_, err := check.zookeeper.Connect(connectString, 10*time.Second)
	if err != nil {
		return err
	}
	defer check.zookeeper.Close()

	topicPath := chroot + "/admin/delete_topics/" + check.config.topicName

	err = createZkNode(check.zookeeper, topicPath, "")
	if err != nil {
		return err
	}

	return check.waitForTopicDeletion(topicPath)
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
	check.close(false)
	return check.connect(false, stop)
}
