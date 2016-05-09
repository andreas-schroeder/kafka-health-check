package check

import "testing"

func Test_WhenAlphanumericTopicNameGiven_validTopicName_ReturnsTrue(t *testing.T) {
	topicName := "validTestTopic123"
	result := validTopicName(topicName)

	if result == false {
		t.Error("Kafka topic name", topicName, "is considered invalid, but is valid")
	}
}

func Test_WhenTopicNameWithSlashGiven_validTopicName_ReturnsFalse(t *testing.T) {
	topicName := "invalid/topicName"
	result := validTopicName(topicName)

	if result == true {
		t.Error("Kafka topic name", topicName, "is considered valid, but is invalid")
	}
}
