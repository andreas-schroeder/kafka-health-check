package check

import (
	"errors"
	"math/rand"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/kafkatest"
	"github.com/optiopay/kafka/proto"
)

func newTestCheck() *HealthCheck {
	config := HealthCheckConfig{
		MessageLength:        100,
		CheckInterval:        1 * time.Millisecond,
		retryInterval:        1 * time.Millisecond,
		CheckTimeout:         5 * time.Millisecond,
		DataWaitInterval:     1 * time.Millisecond,
		NoTopicCreation:      true,
		topicName:            "health-check",
		replicationTopicName: "replication-check",
		brokerID:             1,
		statusServerPort:     8000,
	}

	return &HealthCheck{
		config:                 config,
		partitionID:            0,
		replicationPartitionID: 0,
		randSrc:                rand.NewSource(time.Now().UnixNano()),
	}
}

func mockBroker(check *HealthCheck, ctrl *gomock.Controller) (*MockBrokerConnection, *kafkatest.Broker, *kafkatest.Consumer, kafka.Producer) {
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

func healthyMetadata(topicNames ...string) *proto.MetadataResp {
	var topics []proto.MetadataRespTopic

	for _, name := range topicNames {
		topics = append(topics, proto.MetadataRespTopic{
			Name: name,
			Err:  nil,
			Partitions: []proto.MetadataRespPartition{
				{
					ID:       2,
					Err:      nil,
					Leader:   int32(1),
					Replicas: []int32{1, 2},
					Isrs:     []int32{1, 2},
				},
			},
		})
	}

	return &proto.MetadataResp{
		CorrelationID: int32(1),
		Brokers: []proto.MetadataRespBroker{
			{
				NodeID: int32(2),
				Host:   "10.0.0.5",
				Port:   int32(9092),
			},
			{
				NodeID: int32(1),
				Host:   "localhost",
				Port:   int32(9092),
			},
		},
		Topics: topics,
	}
}

func outOfSyncMetadata(topicNames ...string) *proto.MetadataResp {
	var topics []proto.MetadataRespTopic

	for _, name := range topicNames {
		topics = append(topics, proto.MetadataRespTopic{
			Name: name,
			Err:  nil,
			Partitions: []proto.MetadataRespPartition{
				{
					ID:       2,
					Err:      nil,
					Leader:   int32(1),
					Replicas: []int32{1, 2},
					Isrs:     []int32{2},
				},
			},
		})
	}

	return &proto.MetadataResp{
		CorrelationID: int32(1),
		Brokers: []proto.MetadataRespBroker{
			{
				NodeID: int32(2),
				Host:   "10.0.0.5",
				Port:   int32(9092),
			},
			{
				NodeID: int32(1),
				Host:   "localhost",
				Port:   int32(9092),
			},
		},
		Topics: topics,
	}
}

func healthyZkTopics() []ZkTopic {
	return []ZkTopic{
		{
			Name: "some-topic",
			Partitions: map[int32][]int32{
				2: {2, 1},
			},
		},
	}
}

func replicaMissingZkTopics() []ZkTopic {
	return []ZkTopic{
		{
			Name: "some-topic",
			Partitions: map[int32][]int32{
				2: {1},
			},
		},
	}
}

func partitionMissingZkTopics() []ZkTopic {
	return []ZkTopic{
		{
			Name:       "some-topic",
			Partitions: map[int32][]int32{},
		},
	}
}

func healthyZkBrokers() []int32 {
	return []int32{1, 2}
}

func missingZkBrokers() []int32 {
	return []int32{2}
}

func underReplicatedMetadata() *proto.MetadataResp {
	return &proto.MetadataResp{
		CorrelationID: int32(1),
		Brokers: []proto.MetadataRespBroker{
			{
				NodeID: int32(2),
				Host:   "10.0.0.5",
				Port:   int32(9092),
			},
			{
				NodeID: int32(1),
				Host:   "localhost",
				Port:   int32(9092),
			},
		},
		Topics: []proto.MetadataRespTopic{
			{
				Name: "some-topic",
				Err:  nil,
				Partitions: []proto.MetadataRespPartition{
					{
						ID:       2,
						Err:      nil,
						Leader:   int32(2),
						Replicas: []int32{2},
						Isrs:     []int32{2},
					},
				},
			},
		},
	}
}

func inSyncMetadata() *proto.MetadataResp {
	return &proto.MetadataResp{
		CorrelationID: int32(1),
		Brokers: []proto.MetadataRespBroker{
			{
				NodeID: int32(2),
				Host:   "10.0.0.5",
				Port:   int32(9092),
			},
			{
				NodeID: int32(1),
				Host:   "localhost",
				Port:   int32(9092),
			},
		},
		Topics: []proto.MetadataRespTopic{
			{
				Name: "some-topic",
				Err:  nil,
				Partitions: []proto.MetadataRespPartition{
					{
						ID:       1,
						Err:      nil,
						Leader:   int32(2),
						Replicas: []int32{2, 1},
						Isrs:     []int32{2, 1},
					},
				},
			},
		},
	}
}

func offlineMetadata() *proto.MetadataResp {
	return &proto.MetadataResp{
		CorrelationID: int32(1),
		Brokers: []proto.MetadataRespBroker{
			{
				NodeID: int32(1),
				Host:   "localhost",
				Port:   int32(9092),
			},
		},
		Topics: []proto.MetadataRespTopic{
			{
				Name: "some-topic",
				Err:  nil,
				Partitions: []proto.MetadataRespPartition{
					{
						ID:       1,
						Err:      nil,
						Leader:   int32(2),
						Replicas: []int32{1},
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
			{
				NodeID: int32(2),
				Host:   "10.0.0.5",
				Port:   int32(9092),
			},
		},
	}
}

func metadataWithoutTopic() *proto.MetadataResp {
	return &proto.MetadataResp{
		CorrelationID: int32(1),
		Brokers: []proto.MetadataRespBroker{
			{
				NodeID: int32(1),
				Host:   "localhost",
				Port:   int32(9092),
			},
		},
		Topics: []proto.MetadataRespTopic{},
	}
}

func newZkTestCheck(ctrl *gomock.Controller) (check *HealthCheck, zookeeper *MockZkConnection) {
	check = newTestCheck()
	check.config.zookeeperConnect = "localhost:2181"
	zookeeper = NewMockZkConnection(ctrl)
	check.zookeeper = zookeeper
	return
}

func (zookeeper *MockZkConnection) mockGet(path, data string) *gomock.Call {
	return zookeeper.EXPECT().Get(path).Return([]byte(data), nil, nil)
}

func (zookeeper *MockZkConnection) mockTopicGet(name string) *gomock.Call {
	return zookeeper.EXPECT().Get("/brokers/topics/"+name).Return([]byte(`{"version":1,"partitions":{"0":[1]}}`), nil, nil)
}

func (zookeeper *MockZkConnection) mockSuccessfulPathCreation(path string) *gomock.Call {
	before := zookeeper.EXPECT().Exists(path).Return(false, nil, nil)
	zookeeper.EXPECT().Create(path, gomock.Any(), int32(0), gomock.Any()).Return(path, nil).After(before)
	return before
}

func (zookeeper *MockZkConnection) mockFailingPathCreation(path string) *gomock.Call {
	before := zookeeper.EXPECT().Exists(path).Return(false, nil, nil)
	zookeeper.EXPECT().Create(path, gomock.Any(), int32(0), gomock.Any()).Return("", errors.New("Test error")).After(before)
	return before
}

func (zk *MockZkConnection) mockHealthyMetadata(topics ...string) {
	zk.EXPECT().Connect([]string{"localhost:2181"}, 10*time.Second).Return(nil, nil)
	zk.EXPECT().Children("/brokers/ids").Return([]string{"1", "2"}, nil, nil)
	zk.EXPECT().Children("/brokers/topics").Return(topics, nil, nil)
	for _, topic := range topics {
		zk.EXPECT().Get("/brokers/topics/"+topic).Return([]byte(`{"version":1,"partitions":{"2":[1, 2]}}`), nil, nil)
	}
	zk.EXPECT().Close()
}

func (zookeeper *MockZkConnection) mockLock(path string, ctrl *gomock.Controller) *gomock.Call {
	lock := NewMockZkLock(ctrl)
	before := zookeeper.EXPECT().NewLock(path, gomock.Any()).Return(lock, nil)
	lock.EXPECT().Unlock().After(before)
	return before
}
