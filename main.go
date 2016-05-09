package main

import (
	"github.com/andreas-schroeder/kafka-health-check/check"
	"time"
)

func main() {
	healthCheck := check.New(checkConfiguration)
	healthCheck.ParseCommandLineArguments()
	updateChannel := healthCheck.ServeBrokerHealth()
	healthCheck.CheckHealth(updateChannel, make(chan struct{}))
}

var checkConfiguration = check.HealthCheckConfig{
	MessageLength:    100,
	RetryInterval:    5 * time.Second,
	CheckTimeout:     100 * time.Millisecond,
	DataWaitInterval: 20 * time.Millisecond,
}
