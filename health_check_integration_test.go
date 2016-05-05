package main

import (
	"github.com/golang/mock/gomock"
	"testing"
)

func Test_checkHealth_WhenBrokerInMetadataAndProducedMessageIsConsumed_ReportsHealthy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stop := make(chan struct{})
	defer close(stop)

	check := newTestCheck()
	connection := workingBroker(check, ctrl, check.config.topicName, stop)
	connection.EXPECT().Dial(gomock.Any(), gomock.Any()).Return(nil)
	connection.EXPECT().Consumer(gomock.Any()).Return(check.consumer, nil)
	connection.EXPECT().Producer(gomock.Any()).Return(check.producer)
	connection.EXPECT().Metadata().Return(healthyMetadata(check.config.topicName), nil)

	statusUpdates := make(chan string)
	defer close(statusUpdates)
	go check.checkHealth(statusUpdates, stop)
	status := <-statusUpdates

	if status != healthy {
		t.Errorf("checkHealts reported status as %s, expected %s", status, healthy)
	}
}
