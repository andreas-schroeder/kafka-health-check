package check

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"time"

	logr "github.com/sirupsen/logrus"
)

// ParseCommandLineArguments parses the command line arguments.
func (check *HealthCheck) ParseCommandLineArguments() {
	flag.StringVar(&check.config.brokerHost, "broker-host", "localhost", "ip address or hostname of broker host")
	flag.UintVar(&check.config.brokerID, "broker-id", 0, "id of the Kafka broker to health check")
	flag.UintVar(&check.config.brokerPort, "broker-port", 9092, "Kafka broker port")
	flag.StringVar(&check.config.brokerCACertPath, "broker-ca-cert", "", "Kafka broker TLS CA certificate path")
	flag.StringVar(&check.config.brokerClientCertPath, "broker-client-cert", "", "Kafka broker TLS client certificate path")
	flag.StringVar(&check.config.brokerClientKeyPath, "broker-client-key", "", "Kafka broker TLS client key path")
	flag.UintVar(&check.config.statusServerPort, "server-port", 8000, "port to open for http health status queries")
	flag.StringVar(&check.config.zookeeperConnect, "zookeeper", "", "ZooKeeper connect string (e.g. node1:2181,node2:2181,.../chroot)")
	flag.StringVar(&check.config.topicName, "topic", "", "name of the topic to use - use one per broker, defaults to broker-<id>-health-check")
	flag.StringVar(&check.config.replicationTopicName, "replication-topic", "",
		"name of the topic to use for replication checks - use one per cluster, defaults to broker-replication-check")
	flag.UintVar(&check.config.replicationFailureThreshold, "replication-failures-count", 5,
		"number of replication failures before broker is reported unhealthy")
	flag.DurationVar(&check.config.CheckInterval, "check-interval", 10*time.Second, "how frequently to perform health checks")
	flag.BoolVar(&check.config.NoTopicCreation, "no-topic-creation", false, "disable automatic topic creation and deletion")
	flag.Parse()
	l := log.New(os.Stderr, "", 0)
	valid := check.validateConfig(l)
	if !valid {
		l.Printf("%s usage:\n", os.Args[0])
		flag.PrintDefaults()
		l.Fatal("One or more mandatory command line parameters are missing.")
	} else {
		if check.config.topicName == "" {
			check.config.topicName = fmt.Sprintf("broker-%d-health-check", check.config.brokerID)
			logr.Println("using topic", check.config.topicName, "for broker", check.config.brokerID, "health check")
		}
		if check.config.replicationTopicName == "" {
			check.config.replicationTopicName = "broker-replication-check"
			logr.Println("using topic", check.config.topicName, "for broker", check.config.brokerID, "replication check")
		}
		check.config.retryInterval = check.config.CheckInterval / 2
	}
}

func (check *HealthCheck) validateConfig(l *log.Logger) bool {
	valid := true
	if check.config.zookeeperConnect == "" {
		l.Println("parameter -zookeeper required.")
		valid = false
	}
	if check.config.topicName != "" && !validTopicName(check.config.topicName) {
		l.Println("topic name", check.config.topicName, "is not a valid Kafka topic name.")
		valid = false
	}

	return valid
}

// from https://github.com/apache/kafka/blob/6eacc0de303e4d29e083b89c1f53615c1dfa291e/core/src/main/scala/kafka/common/Topic.scala
var legalTopicPattern = regexp.MustCompile("^[a-zA-Z0-9\\._\\-]+$")

func validTopicName(name string) bool {
	return legalTopicPattern.MatchString(name) && len(name) <= 255
}
