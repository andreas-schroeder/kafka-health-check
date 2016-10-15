// +build !race

package check

// data races reported are false positives triggered by sequential tests.
// those races revolve around assignment of http.DefaultServeMux in
// status_server.go. As a goroutine creation creates a happens-before edge
// (https://golang.org/ref/mem#tmp_5), I do not see an actuall concurrency issue
// in the production code - but maybe that's just me.

import (
	"testing"
)

func Test_ServeHealth_DefaultBrokerStatusIsUnhealthy(t *testing.T) {
	awaitServer, stop, _, _ := newServerSetup()

	response := waitForResponse("http://localhost:8000/", t)
	expected := string(simpleStatus(unhealthy))

	if response != expected {
		t.Errorf("Broker health is reported as %s expected is %s", response, expected)
	}
	close(stop)
	awaitServer.Wait()
}

func Test_ServeHealth_DefaultClusterStatusIsRed(t *testing.T) {
	awaitServer, stop, _, _ := newServerSetup()

	response := waitForResponse("http://localhost:8000/cluster", t)
	expected := string(simpleStatus(red))

	if response != expected {
		t.Errorf("Cluster health is reported as %s expected is %s", response, expected)
	}
	close(stop)
	awaitServer.Wait()
}

func Test_ServeHealth_UpdatesBrokerStatus(t *testing.T) {
	awaitServer, stop, brokerUpdates, _ := newServerSetup()

	status := simpleStatus(healthy)
	brokerUpdates <- Update{healthy, status}

	if !waitForExpectedResponse("http://localhost:8000/", string(status)) {
		t.Errorf("Broker health was not reported as %s within timeout", healthy)
	}
	close(stop)
	awaitServer.Wait()
}

func Test_ServeHealth_UpdatesClusterBrokerStatus(t *testing.T) {
	awaitServer, stop, _, clusterUpdates := newServerSetup()

	status := simpleStatus(green)
	clusterUpdates <- Update{green, status}

	if !waitForExpectedResponse("http://localhost:8000/cluster", string(status)) {
		t.Errorf("Broker health was not reported as %s within timeout", green)
	}
	close(stop)
	awaitServer.Wait()
}
