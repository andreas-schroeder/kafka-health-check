package check

import (
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
)

func Test_checkHealth_WhenBrokerInMetadataAndProducedMessageIsConsumed_ReportsHealthy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stop := make(chan struct{})
	awaitCheck := sync.WaitGroup{}

	check := newTestCheck()
	connection := workingBroker(check, ctrl, check.config.topicName, stop)
	connection.EXPECT().Dial(gomock.Any(), gomock.Any()).Return(nil)
	connection.EXPECT().Consumer(gomock.Any()).Return(check.consumer, nil)
	connection.EXPECT().Producer(gomock.Any()).Return(check.producer)
	connection.EXPECT().Metadata().Return(healthyMetadata(check.config.topicName), nil)
	connection.EXPECT().Close()

	statusUpdates := make(chan string)
	defer close(statusUpdates)

	awaitCheck.Add(1)
	go func() {
		check.CheckHealth(statusUpdates, stop)
		awaitCheck.Done()
	}()

	status := <-statusUpdates
	close(stop)
	awaitCheck.Wait()

	if status != healthy {
		t.Errorf("checkHealts reported status as %s, expected %s", status, healthy)
	}
}
