package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/smallfish/simpleyaml"
)

type version struct {
	Kafka string
	Scala string
}

func main() {
	var healthCheckCommand string
	var baseDir string
	flag.StringVar(&healthCheckCommand, "health-check", "", "kafka health check executable to test")
	flag.StringVar(&baseDir, "base-dir", "", "directory containing the Java and Kafka docker build contexts")
	flag.Parse()

	specs := parseSpecs(baseDir)

	buildDocker("java:8", baseDir+"/docker/java")

	for _, spec := range specs {
		fmt.Print("=== RUN checking compatibility with Kafka ", spec.Kafka, " and Scala ", spec.Scala, "...\n")
		tag := fmt.Sprintf("kafka:%s-%s", spec.Scala, spec.Kafka)
		buildDocker(tag, baseDir+"/docker/kafka", "scala_version="+spec.Scala, "kafka_version="+spec.Kafka)

		runTest(tag, spec, healthCheckCommand)
	}

	fmt.Println("PASS")
}

func runTest(tag string, spec version, healthCheckCommand string) {
	zkID, kafkaID, hcCmd := startAll(tag, healthCheckCommand)

	success := waitForResponse("http://localhost:8000/", "sync")
	if success {
		fmt.Println("--- PASS: Kafka", spec.Kafka, "and Scala", spec.Scala, "broker status reported as in sync.")
	} else {
		fmt.Println("--- FAIL: Kafka", spec.Kafka, "and Scala", spec.Scala, "broker status wasn't reported as in sync within timeout.")

		stopAll(zkID, kafkaID, hcCmd)
		return
	}

	success = waitForResponse("http://localhost:8000/cluster", "green")
	if success {
		fmt.Println("--- PASS: Kafka", spec.Kafka, "and Scala", spec.Scala, "cluster status reported as green.")
	} else {
		fmt.Println("--- FAIL: Kafka", spec.Kafka, "and Scala", spec.Scala, "cluster status wasn't reported as green within timeout.")
	}

	stopAll(zkID, kafkaID, hcCmd)
}

func startAll(tag string, healthCheckCommand string) (zkID string, kafkaID string, hcCmd *exec.Cmd) {
	versions := strings.Split(tag, ":")[1]
	zkID = "zk-test-" + versions
	kafkaID = "kafka-test-" + versions

	// defensive cleanup.
	exec.Command("docker", "stop", kafkaID, zkID).Run()
	exec.Command("docker", "rm", kafkaID, zkID).Run()

	log.Print("Starting ZooKeeper...")
	err := exec.Command("docker", "run", "-d", "--name", zkID, "-p", "2181:2181", tag, "zookeeper").Run()
	if err != nil {
		log.Fatal("Failed to start ZooKeeper: ", err)
	}

	if !waitForZooKeeper() {
		exec.Command("docker", "kill", zkID)
		exec.Command("docker", "rm", zkID)
		log.Fatal("ZooKeeper did not become healthy within timeout.")
	}

	log.Print("Starting Kafka...")
	err = exec.Command("docker", "run", "-d", "--name", kafkaID, "-p", "9092:9092", tag).Run()
	if err != nil {
		log.Fatal("Failed to start Kafka: ", err)
	}

	log.Print("Starting health check...")
	hcCmd = exec.Command(healthCheckCommand, "-broker-id", "1", "-zookeeper", "localhost:2181", "-check-interval", "1s")
	err = hcCmd.Start()
	if err != nil {
		log.Fatal("Failed to start health check: ", err)
	}

	// wait for health check to start.
	time.Sleep(100 * time.Millisecond)

	return
}

func stopAll(zkID string, kafkaID string, hcCmd *exec.Cmd) {
	log.Println("stopping health check...")
	hcCmd.Process.Kill()
	hcCmd.Wait()

	log.Print("Killing docker containers ", kafkaID, " ", zkID, "...")

	exec.Command("docker", "kill", kafkaID, zkID).Run()
	exec.Command("docker", "rm", kafkaID, zkID).Run()
}

func waitForZooKeeper() bool {
	for retries := 10; retries > 0; retries-- {
		time.Sleep(1 * time.Second)
		conn, err := net.Dial("tcp", "localhost:2181")
		if err != nil {
			log.Println("Failed to connect to ZooKeeper:", err)
			continue
		}
		fmt.Fprintf(conn, "ruok")
		status, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil && err != io.EOF {
			log.Println("Failed to read ZooKeeper status:", err)
			continue
		}
		if string(status) == "imok" {
			return true
		}
	}
	return false
}

func waitForResponse(url string, expected string) bool {
	for retries := 10; retries > 0; retries-- {
		if retries < 10 {
			time.Sleep(1 * time.Second)
		}
		res, err := http.Get(url)
		if err != nil {
			log.Println("http get returned error", err, "retrying", retries, "more times...")
			continue
		}
		statusBytes, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Println("reading response returned error", err, "retrying", retries, "more times...")
			continue
		}
		status := strings.TrimSpace(string(statusBytes))

		if status == expected {
			return true
		}
		log.Println("reported status is", status, "retrying", retries, "more times...")
	}
	return false
}

func buildDocker(tag string, dir string, buildArgs ...string) {
	log.Print("Docker Build ", tag, "...")
	args := []string{"build", "-t", tag}
	for _, buildArg := range buildArgs {
		args = append(args, "--build-arg")
		args = append(args, buildArg)
	}
	args = append(args, ".") // build context comes last.

	cmd := exec.Command("docker", args...)
	cmd.Dir = dir
	err := cmd.Run()

	if err != nil {
		log.Fatal("Docker Build ", tag, " failed: ", err)
	}
}

func parseSpecs(baseDir string) (checks []version) {
	source, err := ioutil.ReadFile(baseDir + "/spec.yaml")
	if err != nil {
		log.Fatal("unable to locate and read Kafka/Scala versions file spec.yaml: ", err)
	}
	yaml, err := simpleyaml.NewYaml(source)
	if err != nil {
		log.Fatal("unable to parse Kafka/Scala versions file spec.yaml: ", err)
	}
	specs, err := yaml.Array()
	if err != nil {
		log.Fatal("unable to retrieve list of specs from versions file spec.yaml: ", err)
	}

	checks = make([]version, 0)

	for i := range specs {
		spec := yaml.GetIndex(i)
		kafkaVersion, err := spec.Get("kafka").String()
		if err != nil {
			log.Fatal("unable to access Kafka version in spec no.", (i + 1), " in file spec.yaml: ", err)
		}
		scalaVersions, err := spec.Get("scala").Array()
		if err != nil {
			log.Fatal("unable to retrieve list of Scala versions in spec of Kafka version ", kafkaVersion, ": ", err)
		}
		for _, scalaVersion := range scalaVersions {
			checks = append(checks, version{kafkaVersion, scalaVersion.(string)})
		}
	}

	return
}
