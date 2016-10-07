package check

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"reflect"
	"strconv"
	"time"
)

type ZkTopic struct {
	Name       string
	Partitions []ZkPartition
}

type ZkPartition struct {
	ID       int32
	Replicas []int32 `json:"replica"`
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
	partitionsPath := chroot + "/brokers/topics/" + name
	dataBytes, _, err := zk.Get(partitionsPath)
	if err != nil {
		return nil, err
	}
	partitions, err = parseZkPartitions(dataBytes, partitionsPath)
	return
}

func parseZkPartitions(dataBytes []byte, partitionsPath string) (partitions []ZkPartition, err error) {
	var data interface{}
	err = json.Unmarshal(dataBytes, &data)
	if err != nil {
		return nil, err
	}

	parseError := func(detailsFmt string, a ...interface{}) error {
		details := fmt.Sprintf(detailsFmt, a...)
		return errors.New(fmt.Sprintf(
			"Unexpected partition data content: %s\nPath is %s\nPartition data is %s", details, partitionsPath, string(dataBytes)))
	}

	switch m := data.(type) {
	case map[string]interface{}:
		partitionsAttr, ok := m["partitions"]
		if !ok {
			return nil, parseError("Json attribute 'partitions' not found")
		}
		switch partitionsObj := partitionsAttr.(type) {
		case map[string]interface{}:
			for k, v := range partitionsObj {
				id, err := strconv.Atoi(k)
				partition := ZkPartition{ID: int32(id)}
				if err != nil {
					return nil, parseError("Unable to parse partition ID from %s: %s", id, err.Error())
				}
				switch replicas := v.(type) {
				case []interface{}:
					for _, replicaValue := range replicas {
						switch replica := replicaValue.(type) {
						case float64:
							partition.Replicas = append(partition.Replicas, int32(replica))
						default:
							return nil, parseError("Unable to parse replica id from %s of type %s", replica, reflect.TypeOf(replica))
						}
					}
				default:
					return nil, parseError("Unable to parse replica id array from %v of type %s", replicas, reflect.TypeOf(replicas))
				}
				partitions = append(partitions, partition)
			}
		default:
			return nil, parseError("Json 'partitions' attribute does not contain json object, but %s", reflect.TypeOf(partitionsAttr))
		}
	default:
		return nil, parseError("Data is no json object, but %s", reflect.TypeOf(data))
	}

	return
}
