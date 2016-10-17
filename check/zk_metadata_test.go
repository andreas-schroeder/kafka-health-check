package check

import (
	"github.com/golang/mock/gomock"
	"testing"
)

func Test_zkPartitions_WhenDataParsable_ReturnsParsedPartitions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	zkConn := NewMockZkConnection(ctrl)

	data := `{"version":1,"partitions":{"12":[2,3,1],"8":[1,2,3],"19":[3,1,2],"23":[1,3,2],"4":[3,2,1],"15":[2,1,3],"11":[1,3,2],"9":[2,1,3],"22":[3,2,1],"26":[1,2,3],"13":[3,1,2],"24":[2,3,1],"16":[3,2,1],"5":[1,3,2],"10":[3,2,1],"21":[2,1,3],"6":[2,3,1],"1":[3,1,2],"17":[1,3,2],"25":[3,1,2],"14":[1,2,3],"0":[2,3,1],"20":[1,2,3],"27":[2,1,3],"2":[1,2,3],"18":[2,3,1],"7":[3,1,2],"29":[1,3,2],"3":[2,1,3],"28":[3,2,1]}}`
	zkConn.mockGet("/brokers/topics/test-topic", data)

	topic := ZkTopic{Name: "test-topic"}
	err := zkPartitions(&topic, zkConn, "test-topic", "")

	if err != nil {
		t.Errorf("Parsing produced error: %s", err)
	}

	if len(topic.Partitions) != 30 {
		t.Errorf("Parsing produced %d partitions, expected 30", len(topic.Partitions))
	}

	for id, replicas := range topic.Partitions {
		if len(replicas) != 3 {
			t.Errorf("Partition %d has %d replicas, expected 3", id, len(replicas))
		}
	}
}
