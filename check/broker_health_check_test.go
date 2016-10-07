package check

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/optiopay/kafka"
)

func Test_checkBrokerHealth_WhenProducedMessageIsConsumed_ReturnsHealthy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	stop := make(chan struct{})
	defer close(stop)

	check := newTestCheck()
	workingBroker(check, ctrl, stop)

	status := check.checkBrokerHealth(outOfSyncMetadata())

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

	status := check.checkBrokerHealth(outOfSyncMetadata())

	if status.Status != unhealthy {
		t.Errorf("checkBrokerHealth returned %s, expected %s", status.Status, unhealthy)
	}
}

func Test_checkBrokerHealth_WhenProducedMessageIsConsumedAndInSync_ReturnsInSync(t *testing.T) {
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

func workingBroker(check *HealthCheck, ctrl *gomock.Controller, stop <-chan struct{}) *MockBrokerConnection {
	connection, broker, consumer, _ := mockBroker(check, ctrl)

	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				messages, err := broker.ReadProducers(1 * time.Millisecond)
				if err != nil {
					continue
				}
				for _, message := range messages.Messages {
					consumer.Messages <- message
				}
			}
		}
	}()

	return connection
}

func brokenBroker(check *HealthCheck, ctrl *gomock.Controller) chan struct{} {
	_, broker, consumer, _ := mockBroker(check, ctrl)

	stop := make(chan struct{})
	ticker := time.NewTicker(5 * time.Millisecond)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = broker.ReadProducers(1 * time.Millisecond)
			}
		}
	}()

	go func() {
		select {
		case <-ticker.C:
			consumer.Errors <- kafka.ErrNoData
		case <-stop:
			ticker.Stop()
			return
		}
	}()

	return stop
}
