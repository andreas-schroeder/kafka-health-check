package check

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
)

// ServeHealth answers http queries for broker and cluster health.
func (check *HealthCheck) ServeHealth(brokerUpdates <-chan string, clusterUpdates <-chan string, stop <-chan struct{}) {
	port := check.config.statusServerPort

	statusServer := func(name, path, errorStatus string, updates <-chan string) {
		requests := make(chan chan string)

		// goroutine that encapsulates the current status
		go func() {
			status := errorStatus
			for {
				select {
				case update := <-updates:
					if status != update {
						log.Println(name, "now reported as", update)
						status = update
					}

				case request := <-requests:
					request <- status
				}
			}
		}()

		http.HandleFunc(path, func(writer http.ResponseWriter, request *http.Request) {
			responseChannel := make(chan string)
			requests <- responseChannel
			currentStatus := <-responseChannel
			if currentStatus == errorStatus {
				http.Error(writer, currentStatus, 500)
			} else {
				io.WriteString(writer, currentStatus+"\n")
			}
		})
	}

	http.DefaultServeMux = http.NewServeMux()
	statusServer("broker", "/", unhealthy, brokerUpdates)
	statusServer("cluster", "/cluster", red, clusterUpdates)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatal("Unable to listen to port ", port, ": ", err)
	}

	go func() {
		for {
			select {
			case <-stop:
				listener.Close()
			}
		}
	}()
	http.Serve(listener, nil)
	return
}
