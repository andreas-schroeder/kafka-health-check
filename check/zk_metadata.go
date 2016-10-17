package check

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

type ZkTopic struct {
	Name       string
	Partitions map[int32][]int32 `json:"partitions"`
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
		err := zkPartitions(&topic, zk, topicName, chroot)
		if err != nil {
			return nil, err
		}
		topics = append(topics, topic)
	}
	return
}

func zkPartitions(topic *ZkTopic, zk ZkConnection, name, chroot string) error {
	topicPath := chroot + "/brokers/topics/" + name
	dataBytes, _, err := zk.Get(topicPath)
	if err != nil {
		return err
	}

	return json.Unmarshal(dataBytes, &topic)
}
