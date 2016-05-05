package main

import (
	"time"
)

func main() {
	check := &healthCheck{
		broker:    &kafkaBrokerConnection{},
		zookeeper: &zkConnection{},
		config:    checkConfiguration,
	}
	check.parseCommandLineArguments()
	updateChannel := serveBrokerHealth(check.config.statusServerPort)
	check.checkHealth(updateChannel, make(chan struct{}))
}

var checkConfiguration = healthCheckConfig{
	messageLength:    100,
	retryInterval:    5 * time.Second,
	checkTimeout:     100 * time.Millisecond,
	dataWaitInterval: 20 * time.Millisecond,
}
