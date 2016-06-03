package check

import (
	"fmt"
	"io"
	"log"
	"net/http"
)

// ServeHealth answers http queries for broker and cluster health.
func (check *HealthCheck) ServeHealth() (brokerUpdates chan<- string, clusterUpdates chan<- string) {
	port := check.config.statusServerPort

	statusServer := func(name string, path string, errorStatus string) chan<- string {

		updates := make(chan string, 2)
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

		return updates
	}

	brokerUpdates = statusServer("broker", "/", unhealthy)
	clusterUpdates = statusServer("cluster", "/cluster", red)

	go http.ListenAndServe(fmt.Sprintf(":%d", port), nil)

	return
}
