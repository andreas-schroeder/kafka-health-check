package main

import (
	"errors"
	"github.com/golang/mock/gomock"
	"testing"
)

func Test_tryConnectOnce_WhenConnectSucceeds_GivesConsumerAndProducer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	check := newTestCheck()
	connection, _, consumer, producer := mockBroker(check, ctrl, check.config.topicName)
	connection.EXPECT().Dial(gomock.Any(), gomock.Any()).Return(nil)
	connection.EXPECT().Metadata().Return(healthyMetadata(check.config.topicName), nil)
	connection.EXPECT().Consumer(gomock.Any()).Return(consumer, nil)
	connection.EXPECT().Producer(gomock.Any()).Return(producer)
	connection.EXPECT().Close().MaxTimes(0)
	createIfMissing := false

	err := check.tryConnectOnce(&createIfMissing)

	if err != nil {
		t.Error("expected nil error to be returned, but was", err)
	}
	if check.consumer != consumer {
		t.Error("expected check consumer to be", consumer, "but was", check.config)
	}
	if check.producer != producer {
		t.Error("expected check producer to be", producer, "but was", check.producer)
	}
}

func Test_tryConnectOnce_WhenBrokerConnectFails_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	check := newTestCheck()
	connection, _, _, _ := mockBroker(check, ctrl, check.config.topicName)
	connection.EXPECT().Dial(gomock.Any(), gomock.Any()).Return(errors.New("test error"))
	createIfMissing := false

	err := check.tryConnectOnce(&createIfMissing)

	if err == nil {
		t.Error("expected error to be returned, but was nil")
	}
}

func Test_tryConnectOnce_WhenCreateConsumerFails_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	check := newTestCheck()
	connection, _, _, _ := mockBroker(check, ctrl, check.config.topicName)
	connection.EXPECT().Dial(gomock.Any(), gomock.Any()).Return(nil)
	connection.EXPECT().Metadata().Return(healthyMetadata(check.config.topicName), nil)
	connection.EXPECT().Consumer(gomock.Any()).Return(nil, errors.New("test error"))
	connection.EXPECT().Close()
	createIfMissing := false

	err := check.tryConnectOnce(&createIfMissing)

	if err == nil {
		t.Error("expected error to be returned, but was nil")
	}
}

func Test_getBrokerPartitionId_WhenTopicDoesExist_ReturnsTopicId(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	check := newTestCheck()
	connection, _, _, _ := mockBroker(check, ctrl, check.config.topicName)
	connection.EXPECT().Metadata().Return(healthyMetadata(check.config.topicName), nil)

	createIfMissing := false

	id, err := check.getBrokerPartitionId(&createIfMissing)

	if err != nil {
		t.Error("expected error to be nil, but was", err)
	}

	if id != 2 {
		t.Error("expected partition id to be 2, but was", id)
	}
}

func Test_getBrokerPartitionId_WhenBrokerDoesNotExist_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	check := newTestCheck()
	connection, _, _, _ := mockBroker(check, ctrl, check.config.topicName)
	connection.EXPECT().Metadata().Return(metadataWithoutBroker(), nil)

	createIfMissing := false

	_, err := check.getBrokerPartitionId(&createIfMissing)
	if err == nil {
		t.Error("expected error to be returned, but was nil")
	}
}

func Test_getBrokerPartitionId_WhenTopicDoesNotExistAndMayNotCreateIt_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	check, zookeeper := newZkTestCheck(ctrl)
	connection, _, _, _ := mockBroker(check, ctrl, check.config.topicName)
	connection.EXPECT().Metadata().Return(metadataWithoutTopic(), nil)
	zookeeper.EXPECT().Connect([]string{"localhost:2181"}, gomock.Any()).Return(nil, nil).MaxTimes(0)
	createIfMissing := false

	_, err := check.getBrokerPartitionId(&createIfMissing)
	if err == nil {
		t.Error("expected error to be returned, but was nil")
	}
}

func Test_getBrokerPartitionId_WhenTopicDoesNotExistAndMayCreateIt_CreatesTopic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	check, zookeeper := newZkTestCheck(ctrl)
	connection, _, _, _ := mockBroker(check, ctrl, check.config.topicName)
	connection.EXPECT().Metadata().Return(metadataWithoutTopic(), nil)

	zookeeper.EXPECT().Connect([]string{"localhost:2181"}, gomock.Any()).Return(nil, nil)
	zookeeper.mockSuccessfulPathCreation("/config/topics/health-check")
	zookeeper.mockSuccessfulPathCreation("/brokers/topics/health-check")
	zookeeper.EXPECT().Close()

	createIfMissing := true
	_, err := check.getBrokerPartitionId(&createIfMissing)
	if err == nil {
		t.Error("expected error to be returned, but was nil")
	}
}

func Test_getBrokerPartitionId_WhenTopicDoesNotExistAndCreatingItFails_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	check, zookeeper := newZkTestCheck(ctrl)
	connection, _, _, _ := mockBroker(check, ctrl, check.config.topicName)
	connection.EXPECT().Metadata().Return(metadataWithoutTopic(), nil)

	zookeeper.EXPECT().Connect([]string{"localhost:2181"}, gomock.Any()).Return(nil, errors.New("test error"))
	createIfMissing := true
	_, err := check.getBrokerPartitionId(&createIfMissing)
	if err == nil {
		t.Error("expected error to be returned, but was nil")
	}
}

func Test_createHealthCheckTopic_WhenTopicCreationSuccessful_ReturnsNoError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	check := newTestCheck()
	check.config.zookeeperConnect = "localhost:2181,localhost:2182"
	zookeeper := NewMockZkConnection(ctrl)
	check.zookeeper = zookeeper

	zookeeper.EXPECT().Connect([]string{"localhost:2181", "localhost:2182"}, gomock.Any()).Return(nil, nil)
	zookeeper.mockSuccessfulPathCreation("/config/topics/health-check")
	zookeeper.mockSuccessfulPathCreation("/brokers/topics/health-check")
	zookeeper.EXPECT().Close()

	err := check.createHealthCheckTopic()

	if err != nil {
		t.Error("expected error to be nil, but was", err)
	}
}

func Test_createHealthCheckTopic_WhenZookeeperConnectionFails_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	check, zookeeper := newZkTestCheck(ctrl)
	zookeeper.EXPECT().Connect([]string{"localhost:2181"}, gomock.Any()).Return(nil, errors.New("Test error"))

	err := check.createHealthCheckTopic()

	if err == nil {
		t.Error("expected error to be returned, but was nil")
	}
}

func Test_createHealthCheckTopic_WhenCreatingTopicConfigFails_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	check, zookeeper := newZkTestCheck(ctrl)
	zookeeper.EXPECT().Connect([]string{"localhost:2181"}, gomock.Any()).Return(nil, nil)
	zookeeper.mockFailingPathCreation("/config/topics/health-check")
	zookeeper.EXPECT().Close()

	err := check.createHealthCheckTopic()

	if err == nil {
		t.Error("expected error to be returned, but was nil")
	}
}

func Test_createHealthCheckTopic_WhenCreatingTopicPartitionsFails_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	check, zookeeper := newZkTestCheck(ctrl)
	zookeeper.EXPECT().Connect([]string{"localhost:2181"}, gomock.Any()).Return(nil, nil)
	zookeeper.mockSuccessfulPathCreation("/config/topics/health-check")
	zookeeper.mockFailingPathCreation("/brokers/topics/health-check")
	zookeeper.EXPECT().Close()

	err := check.createHealthCheckTopic()

	if err == nil {
		t.Error("expected error to be returned, but was nil")
	}
}

func Test_createZkNode_WhenNodeCreationSucceeds_ReturnsNoError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	zookeeper := NewMockZkConnection(ctrl)
	zookeeper.EXPECT().Exists("/test/path").Return(false, nil, nil)
	zookeeper.EXPECT().Create("/test/path", []byte("test content"), int32(0), gomock.Any()).Return("/test/path", nil)

	err := createZkNode(zookeeper, "/test/path", "test content")

	if err != nil {
		t.Error("expected error to be nil, but was", err)
	}
}

func Test_createZkNode_WhenExistsCheckFails_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	zookeeper := NewMockZkConnection(ctrl)
	zookeeper.EXPECT().Exists("/test/path").Return(false, nil, errors.New("test error"))

	err := createZkNode(zookeeper, "/test/path", "test content")

	if err == nil {
		t.Error("expected error to be returned, but was nil")
	}
}

func Test_createZkNode_WhenNodeExists_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	zookeeper := NewMockZkConnection(ctrl)
	zookeeper.EXPECT().Exists("/test/path").Return(true, nil, nil)

	err := createZkNode(zookeeper, "/test/path", "test content")

	if err == nil {
		t.Error("expected error to be returned, but was nil")
	}
}

func Test_createZkNode_WhenNodeCreationFails_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	zookeeper := NewMockZkConnection(ctrl)
	zookeeper.EXPECT().Exists("/test/path").Return(false, nil, nil)
	zookeeper.EXPECT().Create("/test/path", []byte("test content"), int32(0), gomock.Any()).Return("", errors.New("test error"))

	err := createZkNode(zookeeper, "/test/path", "test content")

	if err == nil {
		t.Error("expected error to be returned, but was nil")
	}
}
