package check

import (
	"bytes"
	"io"
	"net/http"

	log "github.com/sirupsen/logrus"
)

// ServeHealth answers http queries for broker and cluster health.
func (check *HealthCheck) ServeHealth(brokerUpdates <-chan Update, stop <-chan struct{},brokerID string) {

	statusServer := func(name, path, errorStatus string, updates <-chan Update) {
		requests := make(chan chan Update)

		// goroutine that encapsulates the current status
		go func() {
			status := Update{errorStatus, simpleStatus(errorStatus)}
			for {
				select {
				case update := <-updates:

					if !bytes.Equal(status.Data, update.Data) {
						log.WithField("status", string(update.Data)).Info(name, " now reported as ", update.Status)
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

	statusServer("broker", "/" + brokerID, unhealthy, brokerUpdates)
	log.Println("added " + "/" + brokerID)
	
	return
}

func (check *HealthCheck) ServeHealthCluster(clusterUpdates <-chan Update, stop <-chan struct{}) {

	statusServer := func(name, path, errorStatus string, updates <-chan Update) {
		requests := make(chan chan Update)

		// goroutine that encapsulates the current status
		go func() {
			status := Update{errorStatus, simpleStatus(errorStatus)}
			for {
				select {
				case update := <-updates:

					if !bytes.Equal(status.Data, update.Data) {
						log.WithField("status", string(update.Data)).Info(name, " now reported as ", update.Status)
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

	
	statusServer("cluster", "/cluster", red, clusterUpdates)
	log.Println("added " + "/cluster")
	
	return
}


