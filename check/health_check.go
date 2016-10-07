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
	zookeeper              ZkConnection
	broker                 BrokerConnection
	consumer               kafka.Consumer
	producer               kafka.Producer
	config                 HealthCheckConfig
	partitionID            int32
	replicationPartitionID int32
	randSrc                rand.Source
}

// HealthCheckConfig is the configuration for the health check.
type HealthCheckConfig struct {
	MessageLength               int
	CheckInterval               time.Duration
	CheckTimeout                time.Duration
	DataWaitInterval            time.Duration
	NoTopicCreation             bool
	retryInterval               time.Duration
	topicName                   string
	replicationTopicName        string
	replicationFailureThreshold uint
	brokerID                    uint
	brokerPort                  uint
	zookeeperConnect            string
	statusServerPort            uint
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
	defer check.closeConnection(manageTopic)

	reportUnhealthy := func(err error) {
		log.Println("metadata could not be retrieved, assuming broker unhealthy:", err)
		brokerUpdates <- Update{unhealthy, simpleStatus(unhealthy)}
		clusterUpdates <- Update{red, simpleStatus(red)}
	}

	check.randSrc = rand.NewSource(time.Now().UnixNano())

	log.Info("starting health check loop")
	ticker := time.NewTicker(check.config.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			metadata, err := check.broker.Metadata()
			if err != nil {
				reportUnhealthy(err)
				continue
			}

			zkTopics, zkBrokers, err := check.getZooKeeperMetadata()
			if err != nil {
				reportUnhealthy(err)
				continue
			}

			brokerStatus := check.checkBrokerHealth(metadata)
			brokerUpdates <- newBrokerUpdate(brokerStatus)

			if brokerStatus.Status == unhealthy {
				clusterUpdates <- Update{red, simpleStatus(red)}
				log.Info("closing connection and reconnecting")
				if err := check.reconnect(stop); err != nil {
					log.Info("error while reconnecting:", err)
					return
				}
				log.Info("reconnected")
			} else {
				clusterStatus := check.checkClusterHealth(metadata, zkTopics, zkBrokers)
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

func newBrokerUpdate(status BrokerStatus) Update {
	data, err := json.Marshal(status)
	if err != nil {
		log.Warn("Error while marshaling broker status: %s", err.Error())
		data = simpleStatus(status.Status)
	}
	return Update{status.Status, data}
}

func simpleStatus(status string) []byte {
	return []byte(fmt.Sprintf(`{"status": "%s"}`, status))
}

func contains(a []int32, id int32) bool {
	for _, e := range a {
		if e == id {
			return true
		}
	}
	return false
}

func indexOf(a []int32, id int32) (int, bool) {
	for i, e := range a {
		if e == id {
			return i, true
		}
	}
	return -1, false
}

func sliceDel(a []int32, i int) []int32 {
	copy(a[i:], a[i+1:])
	a[len(a)-1] = 0 // or the zero value of T
	return a[:len(a)-1]
}

func (check *HealthCheck) brokerConfig() kafka.BrokerConf {
	config := kafka.NewBrokerConf("health-check-client")
	config.DialRetryLimit = 1
	config.DialRetryWait = check.config.CheckTimeout
	return config
}

func (check *HealthCheck) consumerConfig() kafka.ConsumerConf {
	config := kafka.NewConsumerConf(check.config.topicName, 0)
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
