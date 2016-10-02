package check

import (
	"testing"
)

func Test_parseZkPartitions_WhenDataParseable(t *testing.T) {
	dataBytes := []byte(`{"version":1,"partitions":{"12":[2,3,1],"8":[1,2,3],"19":[3,1,2],"23":[1,3,2],"4":[3,2,1],"15":[2,1,3],"11":[1,3,2],"9":[2,1,3],"22":[3,2,1],"26":[1,2,3],"13":[3,1,2],"24":[2,3,1],"16":[3,2,1],"5":[1,3,2],"10":[3,2,1],"21":[2,1,3],"6":[2,3,1],"1":[3,1,2],"17":[1,3,2],"25":[3,1,2],"14":[1,2,3],"0":[2,3,1],"20":[1,2,3],"27":[2,1,3],"2":[1,2,3],"18":[2,3,1],"7":[3,1,2],"29":[1,3,2],"3":[2,1,3],"28":[3,2,1]}}`)

	partitions, err := parseZkPartitions(dataBytes, "/path")

	if err != nil {
		t.Errorf("Parsing produced error: %s", err)
	}

	if len(partitions) != 30 {
		t.Errorf("Parsing produced %d partitions, expected 30", len(partitions))
	}

	for _, partition := range partitions {
		if len(partition.Replicas) != 3 {
			t.Errorf("Partition %d has %d replicas, expected 3", partition.ID, len(partition.Replicas))
		}
	}
}
