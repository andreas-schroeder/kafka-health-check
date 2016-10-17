package check

import (
	"testing"

	"github.com/golang/mock/gomock"
)

func Test_checkBrokerHealth_WhenProducedMessageIsConsumed_ReturnsHealthy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	stop := make(chan struct{})
	defer close(stop)

	check := newTestCheck()
	workingBroker(check, ctrl, stop)

	status := check.checkBrokerHealth(outOfSyncMetadata("some-topic"))

	if status.Status != healthy {
		t.Errorf("checkBrokerHealth returned %s, expected %s", status.Status, healthy)
	}
}

func Test_checkBrokerHealth_WhenProducedMessageIsNotConsumed_ReturnsUnhealthy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	check := newTestCheck()
	stop := brokenBroker(check, ctrl)
	defer close(stop)

	status := check.checkBrokerHealth(outOfSyncMetadata("some-topic"))

	if status.Status != unhealthy {
		t.Errorf("checkBrokerHealth returned %s, expected %s", status.Status, unhealthy)
	}
}

func Test_checkBrokerHealth_WhenProducedMessageIsConsumedAndFailsToReplicate_ReturnsUnhealthy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	check := newTestCheck()
	stop := make(chan struct{})
	workingBroker(check, ctrl, stop)
	defer close(stop)

	check.config.replicationFailureThreshold = 0
	check.replicationPartitionID = 2
	status := check.checkBrokerHealth(outOfSyncMetadata(check.config.replicationTopicName))

	if status.Status != unhealthy {
		t.Errorf("checkBrokerHealth returned %s, expected %s", status.Status, unhealthy)
	}
}

func Test_checkBrokerHealth_WhenProducedMessageIsConsumedButOutOfSync_ReturnsHealthy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	check := newTestCheck()
	stop := make(chan struct{})
	workingBroker(check, ctrl, stop)
	defer close(stop)

	status := check.checkBrokerHealth(outOfSyncMetadata("some-topic"))

	if status.Status != healthy {
		t.Errorf("checkBrokerHealth returned %s, expected %s", status.Status, healthy)
	}
}

func Test_checkBrokerHealth_WhenProducedMessageIsConsumedAndInSyncAndReplicates_ReturnsInSync(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	stop := make(chan struct{})
	defer close(stop)

	check := newTestCheck()
	workingBroker(check, ctrl, stop)

	status := check.checkBrokerHealth(inSyncMetadata())

	if status.Status != insync {
		t.Errorf("checkBrokerHealth returned %s, expected %s", status.Status, insync)
	}
}
