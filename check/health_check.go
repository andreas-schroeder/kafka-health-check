package check

import (
	"log"
	"math/rand"
	"time"

	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
)

type healthCheck struct {
	zookeeper   ZkConnection
	broker      BrokerConnection
	consumer    kafka.Consumer
	producer    kafka.Producer
	config      HealthCheckConfig
	partitionID int32
	randSrc     rand.Source
}

// HealthCheckConfig is the configuration for the health check.
type HealthCheckConfig struct {
	MessageLength    int
	CheckInterval    time.Duration
	CheckTimeout     time.Duration
	DataWaitInterval time.Duration
	NoTopicCreation  bool
	retryInterval    time.Duration
	topicName        string
	brokerID         uint
	brokerPort       uint
	zookeeperConnect string
	statusServerPort uint
}

// New creates a new health check with the given config.
func New(config HealthCheckConfig) *healthCheck {
	return &healthCheck{
		broker:    &kafkaBrokerConnection{},
		zookeeper: &zkConnection{},
		config:    config,
	}
}

func (check *healthCheck) brokerConfig() kafka.BrokerConf {
	config := kafka.NewBrokerConf("health-check-client")
	config.DialRetryLimit = 1
	config.DialRetryWait = check.config.CheckTimeout
	return config
}

func (check *healthCheck) consumerConfig() kafka.ConsumerConf {
	config := kafka.NewConsumerConf(check.config.topicName, check.partitionID)
	config.StartOffset = kafka.StartOffsetNewest
	config.RequestTimeout = check.config.CheckTimeout
	config.RetryLimit = 1
	config.RetryWait = check.config.CheckTimeout
	return config
}

func (check *healthCheck) producerConfig() kafka.ProducerConf {
	config := kafka.NewProducerConf()
	config.RequestTimeout = check.config.CheckTimeout
	config.RetryLimit = 1
	return config
}

const (
	insync    = "sync"
	healthy   = "imok"
	unhealthy = "nook"
)

// periodically checks health of the Kafka broker
func (check *healthCheck) CheckHealth(statusUpdates chan<- string, stop <-chan struct{}) {
	manageTopic := !check.config.NoTopicCreation
	err := check.connect(manageTopic, stop)
	if err != nil {
		return
	}
	defer check.close(manageTopic)

	check.randSrc = rand.NewSource(time.Now().UnixNano())

	log.Println("starting broker health check loop")
	ticker := time.NewTicker(check.config.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			status := check.doOneCheck()

			statusUpdates <- status

			if status == unhealthy {
				log.Println("closing connection and reconnecting")
				err := check.reconnect(stop)
				if err != nil {
					log.Println(err)
					return
				}
				log.Println("reconnected")
			}
		case <-stop:
			return
		}
	}
}

// sends one message to the broker partition, wait for it to appear on the consumer.
func (check *healthCheck) doOneCheck() string {
	status := unhealthy
	payload := randomBytes(check.config.MessageLength, check.randSrc)
	message := &proto.Message{Value: []byte(payload)}

	if _, err := check.producer.Produce(check.config.topicName, check.partitionID, message); err != nil {
		log.Println("producer failure - broker unhealthy:", err)
	} else {
		status = check.waitForMessage(message)
	}

	if status == healthy && check.brokerInSync() {
		status = insync
	}

	return status
}

// waits for a message with the payload of the given message to appear on the consumer side.
func (check *healthCheck) waitForMessage(message *proto.Message) string {
	deadline := time.Now().Add(check.config.CheckTimeout)
	for time.Now().Before(deadline) {
		receivedMessage, err := check.consumer.Consume()
		if err != nil {
			if err != kafka.ErrNoData {
				log.Println("consumer failure - broker unhealthy:", err)
				return unhealthy
			}
			if time.Now().Before(deadline) {
				time.Sleep(check.config.DataWaitInterval)
			}
			continue
		}
		if string(receivedMessage.Value) == string(message.Value) {
			return healthy
		}
	}
	return unhealthy
}

// checks whether the broker is in all ISRs of all partitions it replicates.
func (check *healthCheck) brokerInSync() bool {
	metadata, err := check.broker.Metadata()
	if err != nil {
		log.Println("metadata could not be retrieved, broker assumed out of sync:", err)
		return false
	}

	contains := func(arr []int32, id int32) bool {
		for _, e := range arr {
			if e == id {
				return true
			}
		}
		return false
	}

	brokerID := int32(check.config.brokerID)

	for _, topic := range metadata.Topics {
		for _, partition := range topic.Partitions {
			if contains(partition.Replicas, brokerID) && !contains(partition.Isrs, brokerID) {
				return false
			}
		}
	}

	return true
}

// creates a random message payload.
//
// based on the solution http://stackoverflow.com/a/31832326
// provided by http://stackoverflow.com/users/1705598/icza
func randomBytes(n int, src rand.Source) []byte {
	b := make([]byte, n)
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return b
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)
