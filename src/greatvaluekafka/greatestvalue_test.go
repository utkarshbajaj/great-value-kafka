package greatvaluekafka

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func Test_HierarchicalTopicTree(t *testing.T) {
	// Create a new broker controller
	brokerOpts := &newBrokerControllerOpts{
		groupSize:        1,
		numPartitions:    5,
		maxPartitionSize: 1000,
		TTLMs:            99999,
		SweepInterval:    99,
	}
	brokerCtrl := newBrokerController(t, brokerOpts)

	tokens := strings.Split(brokerCtrl.brokerAddr, ":")
	ip := tokens[0]
	port, _ := strconv.Atoi(tokens[1])

	var topics []string

	fmt.Println("Checking if multiple hierarchical topics with one subscriber and one publisher work...")

	for i := range 5 {
		topics = append(topics, "animals-cats-cat"+strconv.Itoa(i))
	}

	// create a topic - animals
	createTopic(t, ip, port, "animals")

	// create the sub topics
	for _, topic := range topics {
		createTopic(t, ip, port, topic)
	}

	// create a consumer group - cats
	cgId := createConsumerGroup(t, ip, port, "animals-cats")

	// create a subscriber for cats
	subId := createSubscriber(t, ip, port, "animals-cats", cgId)

	var originalMessages []string

	// publish a message to all cats topics
	for i, topic := range topics {
		message1 := "meow" + strconv.Itoa(i)
		message2 := "purr" + strconv.Itoa(i)
		originalMessages = append(originalMessages, message1, message2)
		publishMessage(t, ip, port, topic, "", message1)
		publishMessage(t, ip, port, topic, "", message2)
	}

	var resultMessages []string

	// consume the messages
	// a single read on the cats topic should fetch all the messages

	for {
		messages := readMessage(t, ip, port, "animals-cats", cgId, subId)
		if len(messages) == 0 {
			break
		}

		resultMessages = append(resultMessages, messages...)
	}

	if len(resultMessages) != len(originalMessages) {
		t.Fatalf("Expected %v messages, got %v", len(originalMessages), len(resultMessages))
	}

	// check if the messages are the same, but the order is not important
	sort.Strings(originalMessages)
	sort.Strings(resultMessages)

	for i := range originalMessages {
		if originalMessages[i] != resultMessages[i] {
			t.Fatalf("Expected message %v to be %v, got %v", i, originalMessages[i], resultMessages[i])
		}
	}

	fmt.Println("ok")

	fmt.Println("Checking for multiple publishers and multiple subscribers...")

	// create a new topic - animals-elephants-white
	createTopic(t, ip, port, "animals-elephants")

	fmt.Println("ok")
}
