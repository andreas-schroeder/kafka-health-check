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

func (check *HealthCheck) ParseCommandLineArguments(brokerHost string,brokerID uint,brokerPort uint,statusServerPort uint,zookeeperConnect string,topicName string,replicationTopicName string,replicationFailureThreshold uint,CheckInterval time.Duration,NoTopicCreation bool){

	check.config.brokerHost = brokerHost
	check.config.brokerID = brokerID
	check.config.brokerPort = brokerPort
	check.config.statusServerPort = statusServerPort
	check.config.zookeeperConnect = zookeeperConnect
	check.config.topicName = topicName
	check.config.replicationTopicName = replicationTopicName
	check.config.replicationFailureThreshold = replicationFailureThreshold
	check.config.CheckInterval = CheckInterval
	check.config.NoTopicCreation = NoTopicCreation

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
