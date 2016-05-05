# Kafka Health Check

Health checker for Kafka that operates by inserting a message in a dedicated health check topic and waiting for it to
become available on the consumer side.

## Usage

```
kafka-health-check usage:
  -broker-id uint
    	id of the Kafka broker to health check
  -broker-port uint
    	Kafka broker port (default 9092)
  -server-port uint
    	port to open for http health status queries (default 8000)
  -topic string
    	name of the topic to use - use one per broker, defaults to broker-<id>-health-check
  -zookeeper string
    	ZooKeeper connect string (e.g. node1:2181,node2:2181,.../chroot)
```

## Supported Kafka Versions

Tessted with the following Kafka versions:

* 0.9.0.1

## Notable Details on Behavior

* When first started, the checker tries to find the Kafka broker to check in the cluster metadata. Then, it tries to
  find the health check topic, and creates it if missing by communicating directly with ZooKeeper(configuration:
  10 seconds message lifetime, one single partition assigned to the broker to check).

* The check opens a port accepting http requests on a given port (default 8000). The return codes and response bodies
are 200 with `imok` for a healthy broker and 500 with `nook` for an unhealthy one.