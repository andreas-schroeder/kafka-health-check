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
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/hashicorp/go-version"
	"github.com/smallfish/simpleyaml"
	"encoding/json"
)

type versionSpec struct {
	Kafka string
	Scala string
}

func main() {
	var healthCheckCommand string
	var baseDir string
	flag.StringVar(&healthCheckCommand, "health-check", "", "kafka health check executable to test")
	flag.StringVar(&baseDir, "base-dir", "", "directory containing the Java and Kafka docker build contexts")
	flag.Parse()

	determineIp()

	specs := parseVersionSpecs(baseDir)

	successTotal := true
	for _, spec := range specs {
		fmt.Print("=== RUN checking compatibility with Kafka ", spec.Kafka, " and Scala ", spec.Scala, "...\n")
		tag := fmt.Sprintf("kafka:%s-%s", spec.Scala, spec.Kafka)
		buildDocker(tag, baseDir+"/docker/kafka", "scala_version="+spec.Scala, "kafka_version="+spec.Kafka)

		success := runTest(tag, spec, healthCheckCommand)
		successTotal = successTotal && success
	}

	cleanup(baseDir)

	if successTotal {
		fmt.Println("PASS")
	} else {
		fmt.Println("FAIL")
		os.Exit(1)
	}
}

var advertisedHost string

func determineIp() {
	ifaces, err := net.Interfaces()
	if err != nil {
		log.Fatal("Error while fetching network interfaces, aborting.", err)
	}
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip.IsGlobalUnicast() && ip.To4() != nil {
				advertisedHost = ip.To4().String()
				log.Println("using", advertisedHost, "as IP for Kafka containers.")
				return
			}
		}
	}
	log.Fatal("Unable to find a local private IP, aborting.")
}

func runTest(tag string, spec versionSpec, healthCheckCommand string) bool {
	zkID, kafkaID, hcCmd := startAll(tag, healthCheckCommand)

	success := waitForResponse("http://localhost:8000/", "sync")
	if success {
		fmt.Println("--- PASS: Kafka", spec.Kafka, "and Scala", spec.Scala, "broker status reported as in sync.")
	} else {
		fmt.Println("--- FAIL: Kafka", spec.Kafka, "and Scala", spec.Scala, "broker status wasn't reported as in sync within timeout.")

		stopAll(zkID, kafkaID, hcCmd)
		return false
	}

	success = waitForResponse("http://localhost:8000/cluster", "green")
	if success {
		fmt.Println("--- PASS: Kafka", spec.Kafka, "and Scala", spec.Scala, "cluster status reported as green.")
	} else {
		fmt.Println("--- FAIL: Kafka", spec.Kafka, "and Scala", spec.Scala, "cluster status wasn't reported as green within timeout.")
	}

	stopAll(zkID, kafkaID, hcCmd)
	return success
}

func startAll(tag, healthCheckCommand string) (zkID, kafkaID string, hcCmd *exec.Cmd) {
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
	kCmd := exec.Command("docker", "run", "-d",
		"--env", "advertised_host=" + advertisedHost,
		"--name", kafkaID, "-p", "9092:9092",
		"--link", zkID+":zookeeper", tag)
	kCmd.Run()
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

func stopAll(zkID, kafkaID string, hcCmd *exec.Cmd) {
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
		fmt.Fprint(conn, "ruok")
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

type Status struct {
	Status string `json:"status"`
}

func waitForResponse(url, expected string) bool {
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
		var status Status
		err = json.Unmarshal(statusBytes, &status)
		if err != nil {
			log.Println("parsing response", string(statusBytes), "returned error:", err, "retrying", retries, "more times...")
			continue
		}

		if status.Status == expected {
			return true
		}
		log.Println("reported status is", status.Status, "retrying", retries, "more times...")
	}
	return false
}

func buildDocker(tag string, dir string, buildArgs ...string) {
	log.Print("Docker Build ", tag, "...")

	args := dockerBuildArgs(tag, dir, buildArgs)

	cmd := exec.Command("docker", args...)
	cmd.Dir = dir
	err := cmd.Run()

	if err != nil {
		log.Fatal("Docker Build ", tag, " failed: ", err)
	}
}

func dockerBuildArgs(tag string, dir string, buildArgs []string) (args []string) {

	if len(buildArgs) > 0 && mustReplaceBuildArgs() {
		dockerfile, err := replaceBuildArgs(dir, buildArgs)
		if err != nil {
			log.Fatal("Failed to replace build args: ", err)
		}
		args = []string{"build", "-t", tag, "-f", dockerfile, "."}
	} else {
		args = []string{"build", "-t", tag}
		for _, buildArg := range buildArgs {
			args = append(args, "--build-arg")
			args = append(args, buildArg)
		}
		args = append(args, ".") // build context comes last.
	}
	return
}

const dockerfileRelpacedPrefix = "Dockerfile_replaced_"

// cleaning up intermediately created docker files.
func cleanup(baseDir string) {
	filepath.Walk(baseDir+"/docker", func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && strings.HasPrefix(info.Name(), dockerfileRelpacedPrefix) {
			return os.Remove(path)
		}
		return nil
	})
}

func replaceBuildArgs(dir string, buildArgs []string) (fileName string, err error) {
	// mocking build args that were introduced in Docker 1.9
	fileBytes, err := ioutil.ReadFile(dir + "/Dockerfile")
	if err != nil {
		return
	}
	dockerfile := string(fileBytes)
	// drop all ARG lines.
	argRe := regexp.MustCompile("(?m)[\r\n]+^ARG .*$")
	dockerfile = argRe.ReplaceAllString(dockerfile, "")

	// replace all occurrences.
	for _, buildArg := range buildArgs {
		split := strings.Split(buildArg, "=")
		name, value := split[0], split[1]
		dockerfile = strings.Replace(dockerfile, "$"+name, value, -1)
		dockerfile = strings.Replace(dockerfile, "${"+name+"}", value, -1)
	}

	file, err := ioutil.TempFile(dir, dockerfileRelpacedPrefix)
	fileName = path.Base(file.Name())
	if err != nil {
		return
	}

	_, err = file.WriteString(dockerfile)
	if err != nil {
		return
	}
	err = file.Close()

	return
}

// Docker versions prior to 1.9 do not support --build-args
func mustReplaceBuildArgs() bool {
	outBytes, err := exec.Command("docker", "-v").CombinedOutput()
	if err != nil {
		log.Fatal("unable to determine Docker version: ", err)
	}

	minDockerVersion, err := version.NewConstraint(">= 1.9")
	if err != nil {
		log.Fatal("unable to create Docker version constraint: ", err)
	}

	versionString := strings.TrimRight(strings.Split(string(outBytes), " ")[2], ",")
	dockerVersion, err := version.NewVersion(versionString)
	if err != nil {
		log.Fatal("unable to parse Docker version from \"", string(outBytes), "\": ", err)
	}
	// keep your fingers crossed.
	return !minDockerVersion.Check(dockerVersion)
}

func parseVersionSpecs(baseDir string) (specs []versionSpec) {
	source, err := ioutil.ReadFile(baseDir + "/spec.yaml")
	if err != nil {
		log.Fatal("unable to locate and read Kafka/Scala versions file spec.yaml: ", err)
	}
	yaml, err := simpleyaml.NewYaml(source)
	if err != nil {
		log.Fatal("unable to parse Kafka/Scala versions file spec.yaml: ", err)
	}
	specsYaml, err := yaml.Array()
	if err != nil {
		log.Fatal("unable to retrieve list of specs from versions file spec.yaml: ", err)
	}

	specs = make([]versionSpec, 0)

	for i := range specsYaml {
		spec := yaml.GetIndex(i)
		kafkaVersion, err := spec.Get("kafka").String()
		if err != nil {
			log.Fatal("unable to access Kafka version in spec no. ", (i + 1), " in file spec.yaml: ", err)
		}
		scalaVersions, err := spec.Get("scala").Array()
		if err != nil {
			log.Fatal("unable to retrieve list of Scala versions in spec of Kafka version ", kafkaVersion, ": ", err)
		}
		for _, scalaVersion := range scalaVersions {
			specs = append(specs, versionSpec{kafkaVersion, scalaVersion.(string)})
		}
	}

	return
}
