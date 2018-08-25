package main

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"flag"
	"github.com/ltnilaysahu/kafka-health-check/check"
	"strings"
	"net/http"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"net"
	"strconv"
)

func main() {

	var brokerHosts string
	var zookeeperConnect string
	var topics string
	var replicationTopicName string
	var replicationFailureThreshold uint
	var CheckInterval time.Duration 
	var NoTopicCreation bool
	var statusServerPort uint
	var brokerPort uint

	flag.StringVar(&brokerHosts, "broker-hosts", "localhost", "ip address or hostname of broker host")
	flag.UintVar(&brokerPort, "broker-port", 9092, "Kafka broker port")
	flag.UintVar(&statusServerPort, "server-port", 8000, "port to open for http health status queries")
	flag.StringVar(&zookeeperConnect, "zookeeper", "", "ZooKeeper connect string (e.g. node1:2181,node2:2181,.../chroot)")
	flag.StringVar(&topics, "topic", "", "name of the topic to use - use one per broker, defaults to broker-<id>-health-check")
	flag.StringVar(&replicationTopicName, "replication-topic", "",
		"name of the topic to use for replication checks - use one per cluster, defaults to broker-replication-check")
	flag.UintVar(&replicationFailureThreshold, "replication-failures-count", 5,
		"number of replication failures before broker is reported unhealthy")
	flag.DurationVar(&CheckInterval, "check-interval", 5*time.Second, "how frequently to perform health checks")
	flag.BoolVar(&NoTopicCreation, "no-topic-creation", false, "disable automatic topic creation and deletion")

	log.Println("Parsing arguments...")
	flag.Parse()

	var brokerCount int 
	var brokerHostList [] string
	var topicList [] string
	
	brokerHostList = strings.Split(brokerHosts,",")
	topicList = strings.Split(topics,",")
	brokerCount = len(brokerHostList)
	
    log.Println("Total brokers " + strconv.Itoa(brokerCount))
 
 	http.DefaultServeMux = http.NewServeMux()

 	//Limiting max broker to 5 and 1 zookeepercluster
 	var brokerUpdatesList [6]chan  check.Update
 	var healthCheckList [6] *check.HealthCheck

 	clusterUpdates := make(chan check.Update, 2)

 	
 	stop, awaitCheck := addShutdownHook()

 	
	for i := 0 ; i <brokerCount; i++ {
		healthCheckList[i] = check.New(checkConfiguration)
		healthCheckList[i].ParseCommandLineArguments(brokerHostList[i],uint(i),brokerPort,statusServerPort,zookeeperConnect,topicList[i],replicationTopicName,replicationFailureThreshold,CheckInterval,NoTopicCreation)
		brokerUpdatesList[i] = make(chan check.Update, 2)
		healthCheckList[i].ServeHealth(brokerUpdatesList[i], stop,strconv.Itoa(i))
	}
	
	healthCheckList[brokerCount].ServeHealthCluster(clusterUpdates,stop) // for zookeepercluster 

	
	go startHttpServer(statusServerPort,stop)
	
	connectionInterval := 5*time.Second
	ticker := time.NewTicker(connectionInterval)
	allConnected := true
	for {
		select {
			case <-ticker.C:
				allConnected = true
				for i :=0;i <brokerCount ; i++{
					log.Println("Initializing broker number " + strconv.Itoa(i))
					connected := healthCheckList[i].InitializeConnection(stop)
					allConnected = allConnected && connected
				}
			case <- stop:
				ticker.Stop()
				return
			}

		if allConnected{
			ticker.Stop()
			break
		}
	}

	doloop := true

	ticker = time.NewTicker(CheckInterval)
	log.Println("Starting HealthCheck..... ")

	for{

		select {
			case <-stop:
				doloop = false
				ticker.Stop()
			case <-ticker.C:
				for i :=0;i <brokerCount ; i++{
					log.Println("checking broker number " + strconv.Itoa(i))
					healthCheckList[i].CheckHealth(brokerUpdatesList[i], clusterUpdates, stop)
				}
				continue
			}

			if !doloop{
				break
			}
	}

 	awaitCheck.Done()
	log.Println("healthcheck ENDED...")
	
}


func startHttpServer(statusServerPort uint,stop <-chan struct{}){
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", statusServerPort))
	if err != nil {
		log.Fatal("Unable to listen to port ", statusServerPort, ": ", err)
	}

	go func() {
		for {
			select {
			case <-stop:
				listener.Close()
				log.Println("listener closed....")
				return
			}
		}
	}()
	http.Serve(listener, nil)
	return 
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
	CheckTimeout:     100 * time.Millisecond,
	DataWaitInterval: 20 * time.Millisecond,
	MessageLength:    20,
}
