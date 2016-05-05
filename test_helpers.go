package main

import (
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/kafkatest"
	"github.com/optiopay/kafka/proto"
	"math/rand"
	"time"
)

func newTestCheck() *healthCheck {
	config := healthCheckConfig{
		messageLength:    100,
		retryInterval:    1 * time.Millisecond,
		checkInterval:    1 * time.Millisecond,
		checkTimeout:     5 * time.Millisecond,
		dataWaitInterval: 1 * time.Millisecond,
		topicName:        "health-check",
		brokerId:         1,
	}

	return &healthCheck{
		config:  config,
		randSrc: rand.NewSource(time.Now().UnixNano()),
	}
}

func mockBroker(check *healthCheck, ctrl *gomock.Controller, topicName string) (*MockBrokerConnection, *kafkatest.Broker, *kafkatest.Consumer, kafka.Producer) {
	broker := kafkatest.NewBroker()
	consumer := &kafkatest.Consumer{
		Broker:   broker,
		Messages: make(chan *proto.Message),
		Errors:   make(chan error),
	}
	producer := broker.Producer(kafka.NewProducerConf())
	connection := NewMockBrokerConnection(ctrl)
	check.broker = connection
	check.consumer = consumer
	check.producer = producer

	return connection, broker, consumer, producer
}

func healthyMetadata(topicName string) *proto.MetadataResp {
	return &proto.MetadataResp{
		CorrelationID: int32(1),
		Brokers: []proto.MetadataRespBroker{
			proto.MetadataRespBroker{
				NodeID: int32(2),
				Host:   "10.0.0.5",
				Port:   int32(9092),
			},
			proto.MetadataRespBroker{
				NodeID: int32(1),
				Host:   "localhost",
				Port:   int32(9092),
			},
		},
		Topics: []proto.MetadataRespTopic{
			proto.MetadataRespTopic{
				Name: "some-other-topic",
				Err:  nil,
				Partitions: []proto.MetadataRespPartition{
					proto.MetadataRespPartition{
						ID:       1,
						Err:      nil,
						Leader:   int32(2),
						Replicas: []int32{},
						Isrs:     []int32{},
					},
				},
			},
			proto.MetadataRespTopic{
				Name: topicName,
				Err:  nil,
				Partitions: []proto.MetadataRespPartition{
					proto.MetadataRespPartition{
						ID:       2,
						Err:      nil,
						Leader:   int32(1),
						Replicas: []int32{},
						Isrs:     []int32{},
					},
				},
			},
		},
	}
}

func metadataWithoutBroker() *proto.MetadataResp {
	return &proto.MetadataResp{
		CorrelationID: int32(1),
		Brokers: []proto.MetadataRespBroker{
			proto.MetadataRespBroker{
				NodeID: int32(2),
				Host:   "10.0.0.5",
				Port:   int32(9092),
			},
		},
		Topics: []proto.MetadataRespTopic{
			proto.MetadataRespTopic{
				Name: "some-other-topic",
				Err:  nil,
				Partitions: []proto.MetadataRespPartition{
					proto.MetadataRespPartition{
						ID:       1,
						Err:      nil,
						Leader:   int32(2),
						Replicas: []int32{},
						Isrs:     []int32{},
					},
				},
			},
		},
	}
}

func metadataWithoutTopic() *proto.MetadataResp {
	return &proto.MetadataResp{
		CorrelationID: int32(1),
		Brokers: []proto.MetadataRespBroker{
			proto.MetadataRespBroker{
				NodeID: int32(1),
				Host:   "localhost",
				Port:   int32(9092),
			},
		},
		Topics: []proto.MetadataRespTopic{},
	}
}

func newZkTestCheck(ctrl *gomock.Controller) (check *healthCheck, zookeeper *MockZkConnection) {
	check = newTestCheck()
	check.config.zookeeperConnect = "localhost:2181"
	zookeeper = NewMockZkConnection(ctrl)
	check.zookeeper = zookeeper
	return
}

func (zookeeper *MockZkConnection) mockSuccessfulPathCreation(path string) {
	zookeeper.EXPECT().Exists(path).Return(false, nil, nil)
	zookeeper.EXPECT().Create(path, gomock.Any(), int32(0), gomock.Any()).Return(path, nil)
}

func (zookeeper *MockZkConnection) mockFailingPathCreation(path string) {
	zookeeper.EXPECT().Exists(path).Return(false, nil, nil)
	zookeeper.EXPECT().Create(path, gomock.Any(), int32(0), gomock.Any()).Return("", errors.New("Test error"))
}
