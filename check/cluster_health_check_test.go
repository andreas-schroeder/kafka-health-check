package check

import (
	"testing"

	"github.com/golang/mock/gomock"
)

func Test_checkClusterHealth_WhenAllMetadataConsistent_ReportsGreen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	stop := make(chan struct{})
	defer close(stop)

	check := newTestCheck()
	workingBroker(check, ctrl, stop)

	clusterStatus := check.checkClusterHealth(healthyMetadata("some-topic"), healthyZkTopics(), healthyZkBrokers())

	if clusterStatus.Status != green {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus, green)
	}
}

func Test_checkClusterHealth_WhenSomePartitionUnderReplicated_ReportsYellow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	stop := make(chan struct{})
	defer close(stop)

	check := newTestCheck()
	workingBroker(check, ctrl, stop)

	clusterStatus := check.checkClusterHealth(underReplicatedMetadata(), healthyZkTopics(), healthyZkBrokers())

	if clusterStatus.Status != yellow {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus, yellow)
	}
}

func Test_checkClusterHealth_WhenSomePartitionOffline_ReportsRed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	stop := make(chan struct{})
	defer close(stop)

	check := newTestCheck()
	workingBroker(check, ctrl, stop)

	clusterStatus := check.checkClusterHealth(offlineMetadata(), healthyZkTopics(), healthyZkBrokers())

	if clusterStatus.Status != red {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus, red)
	}
}
