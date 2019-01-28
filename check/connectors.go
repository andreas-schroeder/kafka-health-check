package check

import (
	"time"

	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
	"github.com/samuel/go-zookeeper/zk"
)

// BrokerConnection represents a connection to the Kafka broker
type BrokerConnection interface {
	Dial(nodeAddresses []string, conf kafka.BrokerConf) error

	Consumer(conf kafka.ConsumerConf) (kafka.Consumer, error)

	Producer(conf kafka.ProducerConf) kafka.Producer

	Metadata() (*proto.MetadataResp, error)

	Close()
}

// actual implementation of the Kafka broker connection based on optiopay/kafka.
type kafkaBrokerConnection struct {
	broker *kafka.Broker
}

func (connection *kafkaBrokerConnection) Dial(nodeAddresses []string, conf kafka.BrokerConf) error {
	broker, err := kafka.Dial(nodeAddresses, conf)
	if err != nil {
		return err
	}
	connection.broker = broker
	return nil
}

func (connection *kafkaBrokerConnection) Consumer(conf kafka.ConsumerConf) (kafka.Consumer, error) {
	return connection.broker.Consumer(conf)
}

func (connection *kafkaBrokerConnection) Producer(conf kafka.ProducerConf) kafka.Producer {
	return connection.broker.Producer(conf)
}

func (connection *kafkaBrokerConnection) Metadata() (*proto.MetadataResp, error) {
	return connection.broker.Metadata()
}

func (connection *kafkaBrokerConnection) Close() {
	connection.broker.Close()
}

// ZkConnection represents a connection to a ZooKeeper ensemble
type ZkConnection interface {
	Connect(servers []string, sessionTimeout time.Duration) (<-chan zk.Event, error)
	Close()
	Exists(path string) (bool, *zk.Stat, error)
	Set(path string, data []byte, version int32) (*zk.Stat, error)
	Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error)
	Children(path string) ([]string, *zk.Stat, error)
	Get(path string) ([]byte, *zk.Stat, error)
	NewLock(path string, acl []zk.ACL) (ZkLock, error)
}

// Actual implementation based on samuel/go-zookeeper/zk
type zkConnection struct {
	connection *zk.Conn
}

type zkNullLogger struct {
}

func (zkNullLogger) Printf(string, ...interface{}) {}

func (zkConn *zkConnection) Connect(servers []string, sessionTimeout time.Duration) (<-chan zk.Event, error) {
	loggerOption := func(c *zk.Conn) {
		c.SetLogger(zkNullLogger{})
	}
	connection, events, err := zk.Connect(servers, sessionTimeout, loggerOption)
	zkConn.connection = connection
	return events, err
}

func (zkConn *zkConnection) Close() {
	zkConn.connection.Close()
}

func (zkConn *zkConnection) Exists(path string) (bool, *zk.Stat, error) {
	return zkConn.connection.Exists(path)
}

func (zkConn *zkConnection) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	return zkConn.connection.Set(path, data, version)
}

func (zkConn *zkConnection) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	return zkConn.connection.Create(path, data, flags, acl)
}

func (zkConn *zkConnection) Children(path string) ([]string, *zk.Stat, error) {
	return zkConn.connection.Children(path)
}

func (zkConn *zkConnection) Get(path string) ([]byte, *zk.Stat, error) {
	return zkConn.connection.Get(path)
}

type ZkLock interface {
	Unlock() error
}

type zkLock struct {
	lock *zk.Lock
}

// Creates a lock object, in a locked state
func (zkConn *zkConnection) NewLock(path string, acl []zk.ACL) (ZkLock, error) {
	l := zk.NewLock(zkConn.connection, path, acl)
	err := l.Lock()
	return &zkLock{l}, err
}

func (l *zkLock) Unlock() error {
	return l.lock.Unlock()
}
