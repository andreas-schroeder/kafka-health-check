package main

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/andreas-schroeder/kafka-health-check/check"
)

func main() {
	healthCheck := check.New(checkConfiguration)
	healthCheck.ParseCommandLineArguments()

	stop, awaitCheck := addShutdownHook()
	brokerUpdates, clusterUpdates := make(chan check.Update, 2), make(chan check.Update, 2)
	go healthCheck.ServeHealth(brokerUpdates, clusterUpdates, stop)
	healthCheck.CheckHealth(brokerUpdates, clusterUpdates, stop)
	awaitCheck.Done()
}

func addShutdownHook() (chan struct{}, *sync.WaitGroup) {
	stop := make(chan struct{})
	awaitCheck := &sync.WaitGroup{}
	awaitCheck.Add(1)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt)
	signal.Notify(shutdown, syscall.SIGTERM)
	go func() {
		for range shutdown {
			close(stop)
			awaitCheck.Wait()
		}
	}()

	return stop, awaitCheck
}

var checkConfiguration = check.HealthCheckConfig{
	CheckTimeout:     200 * time.Millisecond,
	DataWaitInterval: 20 * time.Millisecond,
	MessageLength:    20,
}
