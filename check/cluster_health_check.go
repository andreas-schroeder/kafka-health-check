package check

import (
	"encoding/json"

	log "github.com/Sirupsen/logrus"
	"github.com/optiopay/kafka/proto"
)

type ClusterStatus struct {
	Status    string           `json:"status"`
	Topics    []TopicStatus    `json:"topics,omitempty"`
	Brokers   []BrokerMetadata `json:"metadata,omitempty"`
	ZooKeeper string           `json:"zookeeper-connection,omitempty"`
}

type TopicStatus struct {
	Topic      string `json:"topic"`
	Status     string
	ZooKeeper  string            `json:"zookeeper,omitempty"`
	Partitions []PartitionStatus `json:"partitions,omitempty"`
}

type PartitionStatus struct {
	ID               int32   `json:"id"`
	Status           string  `json:"status"`
	ZooKeeper        string  `json:"zookeeper,omitempty"`
	OutOfSyncBrokers []int32 `json:"out-of-sync-brokers,omitempty"`
}

type BrokerMetadata struct {
	ID      int32  `json:"broker"`
	Status  string `json:"status"`
	Problem string `json:"problem"`
}

const (
	green  = "green"
	yellow = "yellow"
	red    = "red"
)

// periodically checks health of the Kafka cluster
func (check *HealthCheck) checkClusterHealth() []byte {
	metadata, err := check.broker.Metadata()

	var clusterStatus ClusterStatus = ClusterStatus{Status: red}
	if err != nil {
		log.Println("Error while retrieving metadata:", err)
	} else {
		clusterStatus = ClusterStatus{Status: green}
		zkTopics, zkBrokers, err := check.getZooKeeperMetadata(&clusterStatus)
		if err == nil {
			mStatus := check.checkBrokerMetadata(metadata, zkBrokers, &clusterStatus)
			tStatus := check.checkTopics(metadata, zkTopics, &clusterStatus)
			clusterStatus.Status = worstStatus(tStatus, mStatus)
		}
	}

	data, err := json.Marshal(clusterStatus)

	if err != nil {
		log.Println("Error while marshaling cluster status", err)
		return []byte("{\"status\": \"" + red + "\"}")
	} else {
		return data
	}
}

func (check *HealthCheck) checkBrokerMetadata(metadata *proto.MetadataResp, zkBrokers []int32, cluster *ClusterStatus) (status string) {
	status = green

	var brokersFromMeta []int32
	for _, broker := range metadata.Brokers {
		brokersFromMeta = append(brokersFromMeta, broker.NodeID)
	}

	for _, broker := range brokersFromMeta {
		if !contains(zkBrokers, broker) {
			cluster.Brokers = append(cluster.Brokers, BrokerMetadata{broker, red, "missing in ZooKeeper"})
			status = red
		}
	}

	for _, broker := range zkBrokers {
		if !contains(brokersFromMeta, broker) {
			cluster.Brokers = append(cluster.Brokers, BrokerMetadata{broker, red, "missing in Metadata"})
			status = red
		}
	}

	return
}

func (check *HealthCheck) checkTopics(metadata *proto.MetadataResp, zkTopics []ZkTopic, cluster *ClusterStatus) (status string) {

	zkTopicMap := make(map[string]ZkTopic)
	for _, topic := range zkTopics {
		zkTopicMap[topic.Name] = topic
	}

	status = green
	for _, topic := range metadata.Topics {
		zkTopic, ok := zkTopicMap[topic.Name]
		topicStatus := TopicStatus{Topic: topic.Name, Status: green}
		if !ok {
			topicStatus.Status = red
			topicStatus.ZooKeeper = "Missing ZooKeeper metadata"
		}

		zkPartitionMap := make(map[int32]ZkPartition)
		if ok {
			for _, partition := range zkTopic.Partitions {
				zkPartitionMap[partition.ID] = partition
			}
		}

		for _, partition := range topic.Partitions {
			pStatus := checkPartition(partition, zkPartitionMap, &topicStatus)
			topicStatus.Status = worstStatus(topicStatus.Status, pStatus)
		}
		cluster.Topics = append(cluster.Topics, topicStatus)
		status = worstStatus(topicStatus.Status, status)

		if topicStatus.Status != green {
			log.Infof("Reporting topic %s as %s", topicStatus.Topic, topicStatus.Status)
		}
	}

	return
}

func checkPartition(partition proto.MetadataRespPartition, zkPartitionMap map[int32]ZkPartition, topicStatus *TopicStatus) string {
	status := PartitionStatus{ID: partition.ID, Status: green}

	replicas := partition.Replicas

	zkPartition, ok := zkPartitionMap[partition.ID]
	if !ok {
		status.Status = red
		status.ZooKeeper = "Missing ZooKeeper metadata"
	} else {
		replicas = zkPartition.Replicas
	}

	if len(partition.Isrs) < len(replicas) {
		for _, replica := range replicas {
			if !contains(partition.Isrs, replica) {
				status.OutOfSyncBrokers = append(status.OutOfSyncBrokers, replica)
			}
		}
		status.Status = yellow // partition is under-replicated.
	}
	if len(partition.Isrs) == 0 {
		status.Status = red // partition is offline.
	}
	if status.Status != green {
		log.Infof("Reporting partition %d of topic %s as %s", status.ID, topicStatus.Topic, status.Status)
		topicStatus.Partitions = append(topicStatus.Partitions, status)
	}

	return status.Status
}

func worstStatus(s1 string, s2 string) string {
	switch s1 {
	case green:
		return s2
	case yellow:
		if s2 == green {
			return s1
		}
		return s2
	case red:
		return s1
	}
	return s2
}
