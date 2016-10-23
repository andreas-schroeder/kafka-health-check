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

	clusterStatus := check.checkClusterHealth(
		healthyMetadata("some-topic"), partitionMissingZkTopics(), healthyZkBrokers())

	if clusterStatus.Status != red {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus.Status, red)
	}
}

func Test_checkClusterHealth_WhenSomeReplicaMissingInZK_ReportsRed(t *testing.T) {
	check := newTestCheck()

	clusterStatus := check.checkClusterHealth(
		healthyMetadata("some-topic"), replicaMissingZkTopics(), healthyZkBrokers())

	if clusterStatus.Status != red {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus.Status, red)
	}
}

func Test_checkClusterHealth_WhenSomeTopicMissingInZK_ReportsRed(t *testing.T) {
	check := newTestCheck()

	clusterStatus := check.checkClusterHealth(
		healthyMetadata("some-topic"), []ZkTopic{}, healthyZkBrokers())

	if clusterStatus.Status != red {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus.Status, red)
	}
}

func Test_checkClusterHealth_WhenSomeTopicMissingInMetadata_ReportsRed(t *testing.T) {
	check := newTestCheck()

	clusterStatus := check.checkClusterHealth(
		healthyMetadata( /* no topic */ ), healthyZkTopics(), healthyZkBrokers())

	if clusterStatus.Status != red {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus.Status, red)
	}
}

func Test_worstStatus_WhenGivenYellowGreen_ReturnsYellow(t *testing.T) {
	status := worstStatus(yellow, green)
	if status != yellow {
		t.Errorf("worst status produced %s, expected %s", status, yellow)
	}
}

func Test_worstStatus_WhenGivenYellowRed_ReturnsRed(t *testing.T) {
	status := worstStatus(yellow, red)
	if status != red {
		t.Errorf("worst status produced %s, expected %s", status, red)
	}
}

func Test_worstStatus_WhenGivenSomethingElse_ReturnsSecond(t *testing.T) {
	status := worstStatus("something", "else")
	if status != "else" {
		t.Errorf("worst status produced %s, expected %s", status, "else")
	}
}
