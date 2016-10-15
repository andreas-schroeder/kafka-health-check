package check

import (
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func newServerSetup() (awaitServer *sync.WaitGroup, stop chan struct{}, brokerUpdates, clusterUpdates chan Update) {
	check := newTestCheck()
	awaitServer = &sync.WaitGroup{}
	stop = make(chan struct{})
	brokerUpdates, clusterUpdates = make(chan Update, 2), make(chan Update, 2)
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
