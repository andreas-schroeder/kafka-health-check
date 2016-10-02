package check

import (
	log "github.com/Sirupsen/logrus"
	"math/rand"
	"time"

	"encoding/json"
	"fmt"
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

type Update struct {
	Status string
	Data   []byte
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
func (check *HealthCheck) CheckHealth(brokerUpdates chan<- Update, clusterUpdates chan<- Update, stop <-chan struct{}) {
	manageTopic := !check.config.NoTopicCreation
	err := check.connect(manageTopic, stop)
	if err != nil {
		return
	}
	defer check.close(manageTopic)

	check.randSrc = rand.NewSource(time.Now().UnixNano())

	log.Info("starting health check loop")
	ticker := time.NewTicker(check.config.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			brokerStatus := check.checkBrokerHealth()

			data, err := json.Marshal(brokerStatus)
			if err != nil {
				log.Warn("Error while marshaling broker status: %s", err.Error())
				data = simpleStatus(brokerStatus.Status)
			}

			brokerUpdates <- Update{brokerStatus.Status, data}

			if brokerStatus.Status == unhealthy {
				clusterUpdates <- Update{red, simpleStatus(red)}
				log.Info("closing connection and reconnecting")
				err := check.reconnect(stop)
				if err != nil {
					log.Info("error while reconnecting:", err)
					return
				}
				log.Info("reconnected")
			} else {
				clusterStatus := check.checkClusterHealth()
				data, err := json.Marshal(clusterStatus)
				if err != nil {
					log.Warn("Error while marshaling cluster status: %s", err.Error())
					data = simpleStatus(clusterStatus.Status)
				}

				clusterUpdates <- Update{clusterStatus.Status, data}
			}
		case <-stop:
			return
		}
	}
}

func simpleStatus(status string) []byte {
	return []byte(fmt.Sprintf(`{"status": "%s"}`, status))
}

func contains(arr []int32, id int32) bool {
	for _, e := range arr {
		if e == id {
			return true
		}
	}
	return false
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
