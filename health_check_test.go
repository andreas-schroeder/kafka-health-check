package main

import (
	"github.com/golang/mock/gomock"
	"github.com/optiopay/kafka"
	"testing"
	"time"
)

func Test_doOneCheck_WhenProducedMessageIsConsumed_ReturnsHealthy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	stop := make(chan struct{})
	defer close(stop)

	check := newTestCheck()
	_ = workingBroker(check, ctrl, check.config.topicName, stop)

	status := check.doOneCheck()

	if status != healthy {
		t.Errorf("doOneCheck returned %s, expected %s", status, healthy)
	}
}

func Test_doOneCheck_WhenProducedMessageIsNotConsumed_ReturnsUnhealthy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	check := newTestCheck()
	stop := brokenBroker(check, ctrl, check.config.topicName)
	defer close(stop)

	status := check.doOneCheck()

	if status != unhealthy {
		t.Errorf("doOneCheck returned %s, expected %s", status, unhealthy)
	}
}

func workingBroker(check *healthCheck, ctrl *gomock.Controller, topicName string, stop <-chan struct{}) *MockBrokerConnection {
	connection, broker, consumer, _ := mockBroker(check, ctrl, topicName)

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

func brokenBroker(check *healthCheck, ctrl *gomock.Controller, topicName string) chan struct{} {
	_, broker, consumer, _ := mockBroker(check, ctrl, topicName)

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
