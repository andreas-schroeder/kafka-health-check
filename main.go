package main

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/andreas-schroeder/kafka-health-check/check"
)

func main() {
	healthCheck := check.New(checkConfiguration)
	healthCheck.ParseCommandLineArguments()

	stop, awaitCheck := addShutdownHook()
	brokerUpdates, clusterUpdates := make(chan check.Update, 2), make(chan check.Update, 2)
	awaitCheck.Add(2)
	go healthCheck.ServeHealth(brokerUpdates, clusterUpdates, stop, awaitCheck)
	healthCheck.CheckHealth(brokerUpdates, clusterUpdates, stop, awaitCheck)
	awaitCheck.Wait()
	log.Info("shutdown finished")
}

func addShutdownHook() (chan struct{}, *sync.WaitGroup) {
	stop := make(chan struct{})
	awaitCheck := &sync.WaitGroup{}
	awaitCheck.Add(1)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt)
	signal.Notify(shutdown, syscall.SIGTERM)
	go func() {
		<-shutdown
		close(stop)
		awaitCheck.Done()
	}()

	return stop, awaitCheck
}

var checkConfiguration = check.HealthCheckConfig{
	CheckTimeout:     200 * time.Millisecond,
	DataWaitInterval: 20 * time.Millisecond,
	MessageLength:    20,
}
