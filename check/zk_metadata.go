package check

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

type ZkTopic struct {
	Name       string
	Partitions []ZkPartition
}

type ZkPartition struct {
	ID       int32
	Replicas []int32
}

func (check *HealthCheck) getZooKeeperMetadata() (topics []ZkTopic, brokers []int32, err error) {
	connectString, chroot := zookeeperEnsembleAndChroot(check.config.zookeeperConnect)
	_, err = check.zookeeper.Connect(connectString, 10*time.Second)
	if err != nil {
		err = errors.Wrap(err, "Connecting to ZooKeeper failed")
		return
	}
	defer check.zookeeper.Close()

	brokers, err = zkBrokers(check.zookeeper, chroot)
	if err != nil {
		err = errors.Wrap(err, "Fetching brokers from ZooKeeper failed")
		return
	}

	topics, err = zkTopics(check.zookeeper, chroot)
	if err != nil {
		err = errors.Wrap(err, "Fetching topics from ZooKeeper failed")
		return
	}

	return
}

func zkBrokers(zk ZkConnection, chroot string) (zkBrokers []int32, err error) {
	children, _, err := zk.Children(chroot + "/brokers/ids")
	if err != nil {
		return nil, err
	}
	for _, child := range children {
		id, err := strconv.Atoi(child)
		if err != nil {
			return nil, err
		}
		zkBrokers = append(zkBrokers, int32(id))
	}
	return
}

func zkTopics(zk ZkConnection, chroot string) (topics []ZkTopic, err error) {
	topicsPath := chroot + "/brokers/topics"
	topicNames, _, err := zk.Children(topicsPath)
	if err != nil {
		return nil, err
	}

	for _, topicName := range topicNames {
		topic := ZkTopic{Name: topicName}
		partitions, err := zkPartitions(zk, topicName, chroot)
		if err != nil {
			return nil, err
		}
		topic.Partitions = partitions
		topics = append(topics, topic)
	}
	return
}

func zkPartitions(zk ZkConnection, name, chroot string) (partitions []ZkPartition, err error) {
	topicPath := chroot + "/brokers/topics/" + name
	dataBytes, _, err := zk.Get(topicPath)
	if err != nil {
		return nil, err
	}
	partitions, err = parseZkPartitions(dataBytes, topicPath)
	return
}

func parseZkPartitions(dataBytes []byte, topicPath string) (zkPartitions []ZkPartition, err error) {
	var topic interface{}
	err = json.Unmarshal(dataBytes, &topic)
	if err != nil {
		return nil, err
	}

	parseError := func(detailsFmt string, a ...interface{}) error {
		details := fmt.Sprintf(detailsFmt, a...)
		return errors.New(fmt.Sprintf(
			"Unexpected partition data content: %s\nPath is %s\nPartition data is %s", details, topicPath, string(dataBytes)))
	}

	parseZkReplicas := func(replicas interface{}) (zkReplicas []int32, err error) {
		switch replicas := replicas.(type) {
		default:
			return nil, parseError("Unable to parse replica id array from %v of type %T", replicas, replicas)
		case []interface{}:
			for _, replica := range replicas {
				switch replica := replica.(type) {
				default:
					return nil, parseError("Unable to parse replica id from %s of type %T", replica, replica)
				case float64:
					zkReplicas = append(zkReplicas, int32(replica))
				}
			}
		}
		return
	}

	switch topic := topic.(type) {
	default:
		return nil, parseError("Data is no json object, but %T", topic)
	case map[string]interface{}:
		partitions, ok := topic["partitions"]
		if !ok {
			return nil, parseError("Json attribute 'partitions' not found")
		}
		switch partitions := partitions.(type) {
		default:
			return nil, parseError("Json 'partitions' attribute does not contain json object, but %T", partitions)
		case map[string]interface{}:
			for id, replicas := range partitions {
				id, err := strconv.Atoi(id)
				if err != nil {
					return nil, parseError("Unable to parse partition ID from %s: %s", id, err.Error())
				}
				zkPartition := ZkPartition{ID: int32(id)}
				zkReplicas, err := parseZkReplicas(replicas)
				if err != nil {
					return nil, err
				}
				zkPartition.Replicas = zkReplicas
				zkPartitions = append(zkPartitions, zkPartition)
			}
		}
	}

	return
}
