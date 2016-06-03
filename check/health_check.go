package check

import (
	"log"
	"math/rand"
	"time"

	"github.com/optiopay/kafka"
)

// HealthCheck holds all data required for health checking.
type HealthCheck struct {
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
func New(config HealthCheckConfig) *HealthCheck {
	return &HealthCheck{
		broker:    &kafkaBrokerConnection{},
		zookeeper: &zkConnection{},
		randSrc:   rand.NewSource(time.Now().UnixNano()),
		config:    config,
	}
}

// CheckHealth checks broker and cluster health.
func (check *HealthCheck) CheckHealth(brokerUpdates chan<- string, clusterUpdates chan<- string, stop <-chan struct{}) {
	manageTopic := !check.config.NoTopicCreation
	err := check.connect(manageTopic, stop)
	if err != nil {
		return
	}
	defer check.close(manageTopic)

	check.randSrc = rand.NewSource(time.Now().UnixNano())

	log.Println("starting health check loop")
	ticker := time.NewTicker(check.config.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			brokerStatus := check.checkBrokerHealth()

			brokerUpdates <- brokerStatus

			if brokerStatus == unhealthy {
				clusterUpdates <- red
				log.Println("closing connection and reconnecting")
				err := check.reconnect(stop)
				if err != nil {
					log.Println("error while reconnecting:", err)
					return
				}
				log.Println("reconnected")
			} else {
				clusterUpdates <- check.checkClusterHealth()
			}
		case <-stop:
			return
		}
	}
}

func (check *HealthCheck) brokerConfig() kafka.BrokerConf {
	config := kafka.NewBrokerConf("health-check-client")
	config.DialRetryLimit = 1
	config.DialRetryWait = check.config.CheckTimeout
	return config
}

func (check *HealthCheck) consumerConfig() kafka.ConsumerConf {
	config := kafka.NewConsumerConf(check.config.topicName, check.partitionID)
	config.StartOffset = kafka.StartOffsetNewest
	config.RequestTimeout = check.config.CheckTimeout
	config.RetryLimit = 1
	config.RetryWait = check.config.CheckTimeout
	return config
}

func (check *HealthCheck) producerConfig() kafka.ProducerConf {
	config := kafka.NewProducerConf()
	config.RequestTimeout = check.config.CheckTimeout
	config.RetryLimit = 1
	return config
}
