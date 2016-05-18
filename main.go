package main

import (
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/andreas-schroeder/kafka-health-check/check"
)

func main() {
	healthCheck := check.New(checkConfiguration)
	healthCheck.ParseCommandLineArguments()
	updateChannel := healthCheck.ServeBrokerHealth()

	stop, awaitCheck := addShutdownHook()
	healthCheck.CheckHealth(updateChannel, stop)
	awaitCheck.Done()
}

func addShutdownHook() (chan struct{}, sync.WaitGroup) {
	stop := make(chan struct{})
	awaitCheck := sync.WaitGroup{}
	awaitCheck.Add(1)

	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)
	go func() {
		for _ = range interrupts {
			close(stop)
			awaitCheck.Wait()
		}
	}()

	return stop, awaitCheck
}

var checkConfiguration = check.HealthCheckConfig{
	MessageLength:    20,
	CheckTimeout:     100 * time.Millisecond,
	DataWaitInterval: 20 * time.Millisecond,
}
