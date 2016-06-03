package check

import (
	"testing"

	"github.com/golang/mock/gomock"
)

func Test_checkClusterHealth_WhenAllPartitionsReplicated_ReportsGreen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	stop := make(chan struct{})
	defer close(stop)

	check := newTestCheck()
	connection := workingBroker(check, ctrl, check.config.topicName, stop)
	connection.EXPECT().Metadata().Return(healthyMetadata(check.config.topicName), nil).AnyTimes()

	clusterStatus := check.checkClusterHealth()

	if clusterStatus != green {
		t.Errorf("CheckHealth reported cluster status as %s, expected %s", clusterStatus, green)
	}
}

func Test_checkClusterHealth_WhenSomePartitionUnderreplicated_ReportsYellow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	stop := make(chan struct{})
	defer close(stop)

	check := newTestCheck()
	connection := workingBroker(check, ctrl, check.config.topicName, stop)
	connection.EXPECT().Metadata().Return(outOfSyncMetadata(), nil).AnyTimes()

	clusterStatus := check.checkClusterHealth()

	if clusterStatus != yellow {
		t.Errorf("CheckHealth reported cluster status as %s, expected %s", clusterStatus, yellow)
	}
}

func Test_checkClusterHealth_WhenSomePartitionOffline_ReportsRed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	stop := make(chan struct{})
	defer close(stop)

	check := newTestCheck()
	connection := workingBroker(check, ctrl, check.config.topicName, stop)
	connection.EXPECT().Metadata().Return(offlinecMetadata(), nil).AnyTimes()

	clusterStatus := check.checkClusterHealth()

	if clusterStatus != red {
		t.Errorf("CheckHealth reported cluster status as %s, expected %s", clusterStatus, red)
	}
}
