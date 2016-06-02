package check

import (
	"fmt"
	"io"
	"log"
	"net/http"
)

func (check *healthCheck) ServeBrokerHealth() chan<- string {
	port := check.config.statusServerPort
	statusUpdates := make(chan string, 2)
	statusRequests := make(chan chan string)

	// goroutine that encapsulates the current kafka broker status
	go func() {
		status := unhealthy
		for {
			select {
			case update := <-statusUpdates:
				if status != update {
					switch update {
					case insync:
						log.Println("broker now reported as in sync")
					case unhealthy:
						log.Println("broker now reported as unhealthy")
					case healthy:
						log.Println("broker now reported as healthy")
					}
				}
				status = update
			case request := <-statusRequests:
				request <- status
			}
		}
	}()

	// handler for http requests
	http.HandleFunc("/",
		func(writer http.ResponseWriter, request *http.Request) {
			responseChannel := make(chan string)
			statusRequests <- responseChannel
			currentStatus := <-responseChannel
			if currentStatus == unhealthy {
				http.Error(writer, currentStatus, 501)
			} else {
				io.WriteString(writer, currentStatus+"\n")
			}
		})
	go http.ListenAndServe(fmt.Sprintf(":%d", port), nil)

	return statusUpdates
}
