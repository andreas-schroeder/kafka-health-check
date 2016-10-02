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

	check, zk := newZkTestCheck(ctrl)
	connection := workingBroker(check, ctrl, stop)
	connection.EXPECT().Metadata().Return(healthyMetadata("some-topic"), nil).AnyTimes()
	zk.mockHealthyMetadata("some-topic")

	clusterStatus := check.checkClusterHealth()

	if clusterStatus.Status != green {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus, green)
	}
}

func Test_checkClusterHealth_WhenSomePartitionUnderReplicated_ReportsYellow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	stop := make(chan struct{})
	defer close(stop)

	check, zk := newZkTestCheck(ctrl)
	connection := workingBroker(check, ctrl, stop)
	connection.EXPECT().Metadata().Return(underReplicatedMetadata(), nil).AnyTimes()
	zk.mockHealthyMetadata("some-topic")

	clusterStatus := check.checkClusterHealth()

	if clusterStatus.Status != yellow {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus, yellow)
	}
}

func Test_checkClusterHealth_WhenSomePartitionOffline_ReportsRed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	stop := make(chan struct{})
	defer close(stop)

	check, zk := newZkTestCheck(ctrl)
	connection := workingBroker(check, ctrl, stop)
	connection.EXPECT().Metadata().Return(offlineMetadata(), nil).AnyTimes()
	zk.mockHealthyMetadata("some-topic")

	clusterStatus := check.checkClusterHealth()

	if clusterStatus.Status != red {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus, red)
	}
}
