package check

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
)

// ServeHealth answers http queries for broker and cluster health.
func (check *HealthCheck) ServeHealth(brokerUpdates <-chan Update, clusterUpdates <-chan Update, stop <-chan struct{}) {
	port := check.config.statusServerPort

	statusServer := func(name, path, errorStatus string, updates <-chan Update) {
		requests := make(chan chan Update)

		// goroutine that encapsulates the current status
		go func() {
			status := Update{errorStatus, nil}
			for {
				select {
				case update := <-updates:

					if !bytes.Equal(status.Data, update.Data) {
						log.Println(name, "now reported as", string(update.Data))
						status = update
					}

				case request := <-requests:
					request <- status
				}
			}
		}()

		http.HandleFunc(path, func(writer http.ResponseWriter, request *http.Request) {
			responseChannel := make(chan Update)
			requests <- responseChannel
			currentStatus := <-responseChannel
			if currentStatus.Status == errorStatus {
				http.Error(writer, string(currentStatus.Data), 500)
			} else {
				writer.Write(currentStatus.Data)
				io.WriteString(writer, "\n")
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
