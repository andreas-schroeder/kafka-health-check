package check

import (
	"errors"
	"fmt"
	"github.com/optiopay/kafka/proto"
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"strings"
	"time"
)

func (check *healthCheck) connect(firstConnection bool, stop <-chan struct{}) error {
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
			return errors.New("connect was asked to stop.")
		}
	}
}

func (check *healthCheck) tryConnectOnce(createIfMissing *bool) error {
	pauseTime := check.config.retryInterval
	// connect to kafka cluster
	connectString := []string{fmt.Sprintf("localhost:%d", check.config.brokerPort)}
	err := check.broker.Dial(connectString, check.brokerConfig())
	if err != nil {
		log.Printf("unable to connect to broker, retrying in %s (%s)", pauseTime.String(), err)
		return err
	}

	check.partitionId, err = check.getBrokerPartitionId(createIfMissing)
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
	}

	producer := check.broker.Producer(check.producerConfig())

	check.consumer = consumer
	check.producer = producer
	return nil
}

func (check *healthCheck) getBrokerPartitionId(createIfMissing *bool) (int32, error) {
	brokerId := int32(check.config.brokerId)

	metadata, err := check.broker.Metadata()
	if err != nil {
		return 0, errors.New(fmt.Sprintf("failure retrieving metadata: %s.", err.Error()))
	}

	if !check.brokerExists(metadata) {
		return 0, errors.New(fmt.Sprintf("unable to find broker %d in metadata.", brokerId))
	}

	for _, topic := range metadata.Topics {
		if topic.Name != check.config.topicName {
			continue
		}

		for _, partition := range topic.Partitions {
			if partition.Leader != brokerId {
				continue
			}
			log.Printf("found partition id %d for broker %d", partition.ID, brokerId)
			return partition.ID, nil
		}
	}

	if *createIfMissing {
		err = check.createHealthCheckTopic()
		if err != nil {
			return 0, errors.New(fmt.Sprintf("unable to create health check topic \"%s\": %s.", check.config.topicName, err))
		} else {
			log.Printf("health check topic \"%s\" created for broker %d", check.config.topicName, brokerId)
			*createIfMissing = false
			return 0, errors.New("health check topic created, try again.")
		}
	} else {
		return 0, errors.New(fmt.Sprintf("unable to find topic and parition for broker %d in metadata.", brokerId))
	}
}

func (check *healthCheck) brokerExists(metadata *proto.MetadataResp) bool {
	brokerId := int32(check.config.brokerId)
	for _, broker := range metadata.Brokers {
		if broker.NodeID == brokerId {
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

func (check *healthCheck) createHealthCheckTopic() error {
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

	brokerId := int32(check.config.brokerId)
	partitionAssignment := `{"version":1,"partitions":{"0":[` + fmt.Sprintf("%d", brokerId) + `]}}`
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
		return errors.New(fmt.Sprintf("node %s cannot be created, exists already", path))
	}

	log.Println("creating node ", path)
	flags := int32(0) // permanent node.
	acl := zk.WorldACL(zk.PermAll)
	_, err = zookeeper.Create(path, []byte(content), flags, acl)
	return err
}

func (check *healthCheck) close() {
	check.broker.Close()
}

func (check *healthCheck) reconnect(stop <-chan struct{}) error {
	check.close()
	return check.connect(false, stop)
}
