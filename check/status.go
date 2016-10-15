package check

import "fmt"

const (
	insync    = "sync"
	healthy   = "imok"
	unhealthy = "nook"

	green  = "green"
	yellow = "yellow"
	red    = "red"
)

type BrokerStatus struct {
	Status              string              `json:"status"`
	OutOfSync           []ReplicationStatus `json:"out-of-sync,omitempty"`
	ReplicationFailures uint                `json:"replication-failures,omitempty"`
}

type ReplicationStatus struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

type ClusterStatus struct {
	Status    string           `json:"status"`
	Topics    []TopicStatus    `json:"topics,omitempty"`
	Metadata  []BrokerMetadata `json:"metadata,omitempty"`
	ZooKeeper string           `json:"zookeeper-connection,omitempty"`
}

type TopicStatus struct {
	Topic      string                     `json:"topic"`
	Status     string                     `json:"status"`
	ZooKeeper  string                     `json:"zookeeper,omitempty"`
	Partitions map[string]PartitionStatus `json:"partitions,omitempty"`
}

type PartitionStatus struct {
	Status            string  `json:"status"`
	ZooKeeper         string  `json:"zookeeper,omitempty"`
	OutOfSyncReplicas []int32 `json:"OSR,omitempty"`
}

type BrokerMetadata struct {
	ID      int32  `json:"broker"`
	Status  string `json:"status"`
	Problem string `json:"problem"`
}

func simpleStatus(status string) []byte {
	return []byte(fmt.Sprintf(`{"status": "%s"}`, status))
}
