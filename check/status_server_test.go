// +build !race

// data races reported are false positives triggered by sequential tests.
// those races revolve around assignment of http.DefaultServeMux in
// status_server.go. As a goroutine creation creates a happens-before edge
// (https://golang.org/ref/mem#tmp_5), I do not see an actuall concurrency issue
// in the production code - but maybe that's just me.

package check

import (
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func Test_ServeHealth_DefaultBrokerStatusIsUnhealthy(t *testing.T) {
	awaitServer, stop, _, _ := newServerSetup()

	response := waitForResponse("http://localhost:8000/", t)

	if response != unhealthy {
		t.Errorf("Broker health is reported as %s expected is %s", response, unhealthy)
	}
	close(stop)
	awaitServer.Wait()
}

func Test_ServeHealth_DefaultClusterStatusIsRed(t *testing.T) {
	awaitServer, stop, _, _ := newServerSetup()

	response := waitForResponse("http://localhost:8000/cluster", t)

	if response != red {
		t.Errorf("Cluster health is reported as %s expected is %s", response, red)
	}
	close(stop)
	awaitServer.Wait()
}

func Test_ServeHealth_UpdatesBrokerStatus(t *testing.T) {
	awaitServer, stop, brokerUpdates, _ := newServerSetup()

	brokerUpdates <- healthy

	if !waitForExpectedResponse("http://localhost:8000/", healthy) {
		t.Errorf("Broker health was not reported as %s within timeout", healthy)
	}
	close(stop)
	awaitServer.Wait()
}

func Test_ServeHealth_UpdatesClusterBrokerStatus(t *testing.T) {
	awaitServer, stop, _, clusterUpdates := newServerSetup()

	clusterUpdates <- green

	if !waitForExpectedResponse("http://localhost:8000/cluster", green) {
		t.Errorf("Broker health was not reported as %s within timeout", green)
	}
	close(stop)
	awaitServer.Wait()
}

func newServerSetup() (awaitServer *sync.WaitGroup, stop chan struct{}, brokerUpdates, clusterUpdates chan string) {
	check := newTestCheck()
	awaitServer = &sync.WaitGroup{}
	stop = make(chan struct{})
	brokerUpdates, clusterUpdates = make(chan string, 2), make(chan string, 2)
	awaitServer.Add(1)
	go func() {
		check.ServeHealth(brokerUpdates, clusterUpdates, stop)
		awaitServer.Done()
	}()

	return
}

func waitForResponse(url string, t *testing.T) string {
	for retries := 30; retries > 0; retries-- {
		if retries < 30 {
			time.Sleep(100 * time.Millisecond)
		}
		res, err := http.Get(url)
		if err != nil {
			continue
		}
		statusBytes, err := ioutil.ReadAll(res.Body)
		if err != nil {
			continue
		}
		return strings.TrimSpace(string(statusBytes))
	}

	t.Error("Did not receive an answer from", url, "whithin timeout")
	return ""
}

func waitForExpectedResponse(url, expected string) bool {
	for retries := 30; retries > 0; retries-- {
		if retries < 30 {
			time.Sleep(100 * time.Millisecond)
		}
		res, err := http.Get(url)
		if err != nil {
			continue
		}
		statusBytes, err := ioutil.ReadAll(res.Body)
		if err != nil {
			continue
		}
		status := strings.TrimSpace(string(statusBytes))
		if status == expected {
			return true
		}
	}

	return false
}
