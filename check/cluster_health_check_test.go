package check

import "testing"

func Test_checkClusterHealth_WhenAllMetadataConsistent_ReportsGreen(t *testing.T) {
	check := newTestCheck()

	clusterStatus := check.checkClusterHealth(healthyMetadata("some-topic"), healthyZkTopics(), healthyZkBrokers())

	if clusterStatus.Status != green {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus.Status, green)
	}
}

func Test_checkClusterHealth_WhenSomePartitionUnderReplicated_ReportsYellow(t *testing.T) {
	check := newTestCheck()

	clusterStatus := check.checkClusterHealth(underReplicatedMetadata(), healthyZkTopics(), healthyZkBrokers())

	if clusterStatus.Status != yellow {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus.Status, yellow)
	}
}

func Test_checkClusterHealth_WhenSomePartitionOffline_ReportsRed(t *testing.T) {
	check := newTestCheck()

	clusterStatus := check.checkClusterHealth(offlineMetadata(), healthyZkTopics(), healthyZkBrokers())

	if clusterStatus.Status != red {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus.Status, red)
	}
}

func Test_checkClusterHealth_WhenSomeBrokerMissingInZK_ReportsRed(t *testing.T) {
	check := newTestCheck()

	clusterStatus := check.checkClusterHealth(
		healthyMetadata("some-topic"), healthyZkTopics(), missingZkBrokers())

	if clusterStatus.Status != red {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus.Status, red)
	}
}

func Test_checkClusterHealth_WhenSomePartitionMissingInZK_ReportsRed(t *testing.T) {
	check := newTestCheck()

	zkTopics := []ZkTopic{
		{
			Name: "some-topic",
			Partitions: map[int32][]int32{
				3: {2, 1},
			},
		},
	}

	clusterStatus := check.checkClusterHealth(
		healthyMetadata("some-topic"), zkTopics, healthyZkBrokers())

	if clusterStatus.Status != red {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus.Status, red)
	}
}

func Test_checkClusterHealth_WhenSomeReplicaMissingInZK_ReportsRed(t *testing.T) {
	check := newTestCheck()

	zkTopics := []ZkTopic{
		{
			Name: "some-topic",
			Partitions: map[int32][]int32{
				2: {1},
			},
		},
	}

	clusterStatus := check.checkClusterHealth(
		healthyMetadata("some-topic"), zkTopics, healthyZkBrokers())

	if clusterStatus.Status != red {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus.Status, red)
	}
}

func Test_checkClusterHealth_WhenSomeTopicMissingInZK_ReportsRed(t *testing.T) {
	check := newTestCheck()

	zkTopics := []ZkTopic{}

	clusterStatus := check.checkClusterHealth(
		healthyMetadata("some-topic"), zkTopics, healthyZkBrokers())

	if clusterStatus.Status != red {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus.Status, red)
	}
}

func Test_checkClusterHealth_WhenSomeTopicMissingInMetadata_ReportsRed(t *testing.T) {
	check := newTestCheck()

	zkTopics := []ZkTopic{
		{
			Name: "some-topic",
			Partitions: map[int32][]int32{
				3: {2, 1},
			},
		},
	}

	clusterStatus := check.checkClusterHealth(
		healthyMetadata(), zkTopics, healthyZkBrokers())

	if clusterStatus.Status != red {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus.Status, red)
	}
}
