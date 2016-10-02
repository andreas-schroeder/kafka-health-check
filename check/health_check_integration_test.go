package check

import (
	"sync"
	"testing"

	"encoding/json"
	"github.com/golang/mock/gomock"
)

func Test_checkHealth_WhenBrokerInMetadataAndProducedMessageIsConsumed_ReportsHealthy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stop := make(chan struct{})
	awaitCheck := sync.WaitGroup{}

	check, zk := newZkTestCheck(ctrl)
	connection := workingBroker(check, ctrl, stop)
	connection.EXPECT().Dial(gomock.Any(), gomock.Any()).Return(nil)
	connection.EXPECT().Consumer(gomock.Any()).Return(check.consumer, nil)
	connection.EXPECT().Producer(gomock.Any()).Return(check.producer)
	connection.EXPECT().Metadata().Return(healthyMetadata(check.config.topicName), nil).AnyTimes()
	connection.EXPECT().Close()
	zk.mockHealthyMetadata(check.config.topicName)

	brokerUpdates := make(chan string)
	defer close(brokerUpdates)

	clusterUpdates := make(chan string)
	defer close(clusterUpdates)

	awaitCheck.Add(1)
	go func() {
		check.CheckHealth(brokerUpdates, clusterUpdates, stop)
		awaitCheck.Done()
	}()

	brokerStatus := <-brokerUpdates
	var clusterStatus ClusterStatus
	json.Unmarshal([]byte(<-clusterUpdates), &clusterStatus)
	close(stop)
	awaitCheck.Wait()

	if brokerStatus != insync {
		t.Errorf("CheckHealth reported broker status as %s, expected %s", brokerStatus, insync)
	}

	if clusterStatus.Status != green {
		t.Errorf("CheckHealth reported cluster status as %v, expected %s", clusterStatus, green)
	}
}
