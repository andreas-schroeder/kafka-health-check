# Kafka Health Check

Health checker for Kafka brokers and clusters that operates by checking whether:

* a message inserted in a dedicated health check topic becomes available for consumers,
* the broker can stay in the ISR of a replication check topic,
* the broker is in the in-sync replica set for all partitions it replicates,
* under-replicated partitions exist,
* out-of-sync replicas exist,
* offline partitions exist, and
* the metadata of the cluster and the ZooKeeper metadata are consistent with each other.

## Status
[![Build Status](https://travis-ci.org/andreas-schroeder/kafka-health-check.svg?branch=master)](https://travis-ci.org/andreas-schroeder/kafka-health-check)

## Usage

```
kafka-health-check usage:
  -broker-id uint
    	id of the Kafka broker to health check (default 0)
  -broker-port uint
    	Kafka broker port (default 9092)
  -check-interval duration
    	how frequently to perform health checks (default 10s)
  -no-topic-creation
    	disable automatic topic creation and deletion
  -replication-failures-count uint
    	number of replication failures before broker is reported unhealthy (default 5)
  -replication-topic string
    	name of the topic to use for replication checks - use one per cluster, defaults to broker-replication-check
  -server-port uint
    	port to open for http health status queries (default 8000)
  -topic string
    	name of the topic to use - use one per broker, defaults to broker-<id>-health-check
  -zookeeper string
    	ZooKeeper connect string (e.g. node1:2181,node2:2181,.../chroot)
```

## Broker Health

Broker health can be queried at `/`:

```
$ curl -s localhost:8000/
{"status":"sync"}
```

Return codes and status values are:
* `200` with `sync` for a healthy broker that is fully in sync with all leaders.
* `200` with `imok` for a healthy broker that replays messages of its health
                    check topic, but is not fully in sync.
* `500` with `nook` for an unhealthy broker that fails to replay messages in its health
  check topic within [200 milliseconds](./main.go#L43) or if it fails to stay in the ISR
  of the replication check topic for more checks than `replication-failures-count` (default 5).


The returned json contains details about replicas the broker is lagging behind:

```
$ curl -s localhost:8000/
{"status":"imok","out-of-sync":[{"topic":"mytopic","partition":0}],"replication-failures":1}
```

## Cluster Health

Cluster health can be queried at `/cluster`:

```
$ curl -s localhost:8000/cluster
{"status":"green"}
```

Return codes and status values are:
* `200` with `green`  if all replicas of all partitions of all topics are in sync and metadata is consistent.
* `200` with `yellow` if one or more partitions are under-replicated and metadata is consistent.
* `500` with `red` if one or more partitions are offline or metadata is inconsistent.

The returned json contains details about metadata status and partition replication:

```
$ curl -s localhost:8000/cluster
{"status":"yellow","topics":[
  {"topic":"mytopic","Status":"yellow","partitions":{
      "2":{"status":"yellow","OSR":[3]},
      "1":{"status":"yellow","OSR":[3]}
  }}
]}
```

The fields for additional info and structures are:
* `topics` for topic replication status: `[{"topic":"mytopic","Status":"yellow","partitions":{"2":{"status":"yellow","OSR":[3]}}}]`
   In this data, `OSR` means out-of-sync replica and contains the list of all brokers that are not in the ISR.
* `metadata` for inconsistencies between ZooKeeper and Kafka metadata: `[{"broker":3,"status":"red","problem":"Missing in ZooKeeper"}]`
* `zookeeper` for problems with ZooKeeper connection or data, contains a single string: `"Fetching brokers failed: ..."`

## Supported Kafka Versions

Tested with the following Kafka versions:

* 0.10.0.1
* 0.10.0.0
* 0.9.0.1
* 0.9.0.0

Kafka 0.8 is not supported.

see the [compatibility spec](./compatibility/spec.yaml) for the full list of executed compatibility checks.
To execute the compatibility checks, run `make compatibility`. Running the checks require [Docker](https://www.docker.com/).

## Building

Run `make` to build after running `make deps` to restore the dependencies using [govendor](https://github.com/kardianos/govendor).

### Prerequisites

* Make to run the [Makefile](Makefile)
* [Go 1.7](https://golang.org/dl/) since it's written in Go


## Notable Details on Health Check Behavior

* When first started, the check tries to find the Kafka broker to check in the cluster metadata. Then, it tries to
  find the health check topic, and creates it if missing by communicating directly with ZooKeeper(configuration:
  10 seconds message lifetime, one single partition assigned to the broker to check).
  This behavior can be disabled by using `-no-topic-creation`.
* The check also creates one replication check topic for the whole cluster. This topic is expanded to all brokers
  that are checked.
* When shutting down, the check deletes to health check topic partition by communicating directly with ZooKeeper.
  It also shrinks the partition assignment of the replication check topic, and deletes it when stopping the last
  health check process. This behavior can be disabled by using `-no-topic-creation`.
* The check will try to create the health check and replication check topics only on its first connection after startup.
  If the topic disappears later while the check is running, it will not try to re-create its topics.
* If the broker health check fails, the cluster health will be set to `red`.
* For each check pass, the Kafka cluster metadata is fetched from ZooKeeper, i.e. the full data on brokers and topic
  partitions with replicas.
