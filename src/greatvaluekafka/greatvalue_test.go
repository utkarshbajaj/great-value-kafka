package greatvaluekafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

var autoIncrement int = 0

type BrokerController struct {
	Broker     *Broker
	groupSize  int
	brokerAddr string

	rpcClient *rpc.Client
}

type newBrokerControllerOpts struct {
	groupSize        int
	numPartitions    int
	maxPartitionSize int
	TTLMs            int
	SweepInterval    int
}

func newBrokerController(t *testing.T, opts *newBrokerControllerOpts) *BrokerController {
	// Create a random port for the broker
	port := rand.Intn(20000) + 20000

	// Create a random rpc path and debug path
	rpcPath := "/rpc" + strconv.Itoa(autoIncrement)
	debugPath := "/debug" + strconv.Itoa(autoIncrement)
	autoIncrement++

	brokerAddr := fmt.Sprintf("127.0.0.1:%v", port)
	ctrlAddr := fmt.Sprintf("127.0.0.1:%v", port+1)

	brokerCtrl := &BrokerController{
		Broker: NewBroker(&BrokerOpts{
			BrokerIndex:      0,
			BrokerAddr:       brokerAddr,
			ControlAddr:      ctrlAddr,
			NumPartitions:    opts.numPartitions,
			MaxPartitionSize: opts.maxPartitionSize,
			TTLMs:            opts.TTLMs,
			RPCPath:          rpcPath,
			DebugPath:        debugPath,
			SweepInterval:    opts.SweepInterval,
		}),
		groupSize:  opts.groupSize,
		brokerAddr: brokerAddr,
	}

	var err error
	brokerCtrl.rpcClient, err = rpc.DialHTTPPath("tcp", ctrlAddr, rpcPath)
	if err != nil {
		t.Fatalf("Failed to dial the RPC: %v", err)
	}

	brokerCtrl.rpcClient.Call("BrokerRPC.Activate", struct{}{}, nil)

	t.Cleanup(func() {
		brokerCtrl.rpcClient.Call("BrokerRPC.Terminate", struct{}{}, nil)
	})

	return brokerCtrl
}

func sendHttpRequest(t *testing.T, ip string, port int, endpoint string, method string, body []byte) (string, int) {
	// Create the URL endpoint
	url := "http://" + ip + ":" + strconv.Itoa(port) + endpoint

	// Create the HTTP request
	req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("Failed to create HTTP request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Send the HTTP sendHttpRequest
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to send HTTP request: %v", err)
	}

	defer resp.Body.Close()

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read HTTP response body: %v", err)
	}

	return string(body), resp.StatusCode
}

func createTopic(t *testing.T, ip string, port int, topicName string) {
	// Create the URL endpoint
	url := "/topics"
	reqBody := []byte(`{"name": "` + topicName + `"}`)
	_, statusCode := sendHttpRequest(t, ip, port, url, "POST", reqBody)
	if statusCode != http.StatusCreated && statusCode != http.StatusOK {
		t.Fatalf("Failed to create topic %v", topicName)
	}

	t.Logf("Created topic %v", topicName)
}

func createConsumerGroup(t *testing.T, ip string, port int, topicName string) string {
	// Create the URL endpoint
	url := "/topics/" + topicName + "/subscribe"

	// Send the HTTP request
	body, statusCode := sendHttpRequest(t, ip, port, url, "POST", []byte("{}"))
	if statusCode != http.StatusCreated {
		t.Fatalf("Failed to create consumer group for topic %v", topicName)
	}

	t.Logf("Created consumer group for topic %v", topicName)

	return string(body)
}

func createSubscriber(t *testing.T, ip string, port int, topicName string, consumerGroupId string) string {
	// Create the URL endpoint
	url := "/topics/" + topicName + "/consumer-groups/" + consumerGroupId + "/subscribe"

	// Send the HTTP request
	body, statusCode := sendHttpRequest(t, ip, port, url, "POST", []byte("{}"))
	if statusCode != http.StatusCreated {
		t.Logf("body: %v, statusCode: %v", string(body), statusCode)
		t.Fatalf("Failed to create subscriber for topic %v", topicName)
	}
	t.Logf("Created subscriber for topic %v", topicName)

	return string(body)
}

func publishMessage(t *testing.T, ip string, port int, topicName string, key string, message string) {
	// Create the URL endpoint
	url := "/topics/" + topicName + "/publish"
	reqBody := []byte(`{"key": "` + key + `", "message": "` + message + `"}`)

	// Send the HTTP request
	body, statusCode := sendHttpRequest(t, ip, port, url, "POST", reqBody)
	if statusCode != http.StatusAccepted {
		t.Logf("body: %v, statusCode: %v", string(body), statusCode)
		t.Fatalf("Failed to publish message for topic %v", topicName)
	}
	t.Logf("Published message for topic %v", topicName)
}

func readMessage(t *testing.T, ip string, port int, topicName string, consumerGroupId string, subscriberId string) []string {
	// Create the URL endpoint
	url := "/topics/" + topicName + "/consumer-groups/" + consumerGroupId + "/subscribers/" + subscriberId

	// Send the HTTP request
	body, statusCode := sendHttpRequest(t, ip, port, url, "GET", []byte("{}"))

	if statusCode != http.StatusOK {
		t.Fatalf("Failed to read message for topic %v", topicName)
	}

	// body is a json array of messages, read it
	var messages []string
	err := json.Unmarshal([]byte(body), &messages)
	if err != nil {
		t.Fatalf("Failed to unmarshal message for topic %v", topicName)
		return nil
	}
	return messages
}

// TODO: Add tests for this
// Test that the endpoints are working well
// Test that you can deactivate and activate the broker and the HTTP request behave the same way
// func TestFinalSetup(t *testing.T) {
// 	broker := NewBroker(&BrokerOpts{
// 		BrokerIndex: 0,
// 		BrokerAddr:  "127.0.0.1:9092",
// 		ControlAddr: "127.0.0.1:9093",
// 	})
// 	go broker.Activate()
//  fmt.Println("Passed test 0")
// }

// TestFinalSinglePublishSingleSubscribe tests a single publish and single subscribe
// with one partition. This should work for multiple messages.
func TestFinalSinglePublishSingleSubscribe(t *testing.T) {
	// This creates a broker controller
	// It activates the broker to receive requests from clients
	brokerCtrl := newBrokerController(t, &newBrokerControllerOpts{
		groupSize:        1,
		numPartitions:    1,
		maxPartitionSize: 1000,
		TTLMs:            99999,
		SweepInterval:    99,
	})

	// get the ip and the port of the broker addr
	tokens := strings.Split(brokerCtrl.brokerAddr, ":")
	ip := tokens[0]
	port, _ := strconv.Atoi(tokens[1])

	topicName := "cats"

	// create a topic
	createTopic(t, ip, port, topicName)

	// create a consumer group
	cgId := createConsumerGroup(t, ip, port, topicName)

	// create a subscriber
	subId := createSubscriber(t, ip, port, topicName, cgId)

	// publish a message
	publishMessage(t, ip, port, topicName, "", "meow")

	// read the message
	messages := readMessage(t, ip, port, topicName, cgId, subId)

	if len(messages) != 1 {
		t.Fatalf("Expected 1 message, got %v", len(messages))
	}

	if messages[0] != "meow" {
		t.Fatalf("Expected message to be 'meow', got %v", messages[0])
	}

	fmt.Println("Passed test 1")
}

// startPublisher creates a goroutine that publishes messages with the given parameters
func startPublisher(t *testing.T, ip string, port int, topicName string, publisherName string, numMessages int) {
	go func() {
		for i := 0; i < numMessages; i++ {
			publishMessage(t, ip, port, topicName, "", publisherName+"-meow"+strconv.Itoa(i))
			time.Sleep(69 * time.Millisecond)
		}
	}()
}

// startConsumer creates a goroutine that consumes messages and adds them to the provided channel
func startConsumer(ctx context.Context, t *testing.T, ip string, port int, topicName string, consumerGroupId string, subscriberId string, messagesChan chan<- string) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return // gracefully exit goroutine
			default:
				messages := readMessage(t, ip, port, topicName, consumerGroupId, subscriberId)
				for _, msg := range messages {
					messagesChan <- msg
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()
}

// startConsumerGroup creates a goroutine for each subscriber in a consumer group and collects their messages
func startConsumerGroup(ctx context.Context, t *testing.T, ip string, port int, topicName string, cgId string, subs []string, messagesChan chan<- string) {
	for _, subId := range subs {
		startConsumer(ctx, t, ip, port, topicName, cgId, subId, messagesChan)
	}
}

func TestFinalMultiplePublishersMultipleSubscribers(t *testing.T) {
	/*
		PLAN:
		- first test 1 CG, 1 sub, 1 partition
		- then test 1 CG, 1 sub, 2 partition
		- then test 1 CG, 5 subs, 5 partitions, parallel
		- then test 2 CG (2, 4), 5 partitions, both concurrent
		- then test 3 CG (1,1,1) 5 partitions, both concurrent
	*/
	topicName := "cats"

	// 1 CG, 1 sub, 1 partition
	brokerCtrl0 := newBrokerController(t, &newBrokerControllerOpts{
		groupSize:        1,
		numPartitions:    1,
		maxPartitionSize: 1000,
		TTLMs:            99999,
		SweepInterval:    99,
	})

	tokens := strings.Split(brokerCtrl0.brokerAddr, ":")
	ip := tokens[0]
	port, _ := strconv.Atoi(tokens[1])

	// create a topic
	createTopic(t, ip, port, topicName)

	// create a consumer group
	cgId := createConsumerGroup(t, ip, port, topicName)

	// create a subscriber
	subId := createSubscriber(t, ip, port, topicName, cgId)

	// publish 10 messages
	n := 10
	originalMessages0 := make([]string, n)
	for i := 0; i < n; i++ {
		originalMessages0[i] = "meow" + strconv.Itoa(i)
		publishMessage(t, ip, port, topicName, "", originalMessages0[i])
	}

	// read the messages
	messages0 := readMessage(t, ip, port, topicName, cgId, subId)

	if len(messages0) != n {
		t.Fatalf("Expected %v messages, got %v", n, len(messages0))
	}

	// check if the message slices are the same
	// this time order does matter, its just one partition
	for i := range originalMessages0 {
		if originalMessages0[i] != messages0[i] {
			t.Fatalf("Expected message %v to be %v, got %v", i, originalMessages0[i], messages0[i])
		}
	}

	fmt.Println("Passed test 2.0")

	// ========== 1 CG, 1 sub, 2 partition ==========
	brokerCtrl1 := newBrokerController(t, &newBrokerControllerOpts{
		groupSize:        1,
		numPartitions:    2,
		maxPartitionSize: 1000,
		TTLMs:            99999,
		SweepInterval:    99,
	})

	tokens = strings.Split(brokerCtrl1.brokerAddr, ":")
	ip = tokens[0]
	port, _ = strconv.Atoi(tokens[1])

	// create a topic
	createTopic(t, ip, port, topicName)

	// create a consumer group
	cgId = createConsumerGroup(t, ip, port, topicName)

	numSubscribers := 2

	// create multiple subscribers
	subs := make([]string, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		subs[i] = createSubscriber(t, ip, port, topicName, cgId)
	}

	var originalMessages1 []string

	// Publish 10 messages using a loop
	for i := 0; i < 10; i++ {
		message := "meow" + strconv.Itoa(i)
		originalMessages1 = append(originalMessages1, message)
		publishMessage(t, ip, port, topicName, "", message)
	}

	var messages1 []string

	// read the messages
	for i := 0; i < numSubscribers; i++ {
		messages1 = append(messages1, readMessage(t, ip, port, topicName, cgId, subs[i])...)
	}

	if len(messages1) != 10 {
		t.Fatalf("Expected 10 messages, got %v", len(messages1))
	}

	// check if the message slices are the same
	// ordering does not matter, so we can sort them
	sort.Strings(originalMessages1)
	sort.Strings(messages1)

	for i := range originalMessages1 {
		if originalMessages1[i] != messages1[i] {
			t.Fatalf("Expected message %v to be %v, got %v", i, originalMessages1[i], messages1[i])
		}
	}

	fmt.Println("Passed test 2.1")

	// ========== 1 CG, 5 subs, 5 partitions ===========
	brokerCtrl2 := newBrokerController(t, &newBrokerControllerOpts{
		groupSize:        1,
		numPartitions:    5,
		maxPartitionSize: 3000,
		TTLMs:            99999,
		SweepInterval:    99,
	})

	tokens = strings.Split(brokerCtrl2.brokerAddr, ":")
	ip = tokens[0]
	port, _ = strconv.Atoi(tokens[1])

	// create a topic
	createTopic(t, ip, port, topicName)

	// create a consumer group
	cgId = createConsumerGroup(t, ip, port, topicName)

	// create 5 subscribers
	subs = make([]string, 5)
	for i := 0; i < 5; i++ {
		subs[i] = createSubscriber(t, ip, port, topicName, cgId)
	}

	messagesChan := make(chan string, 200)

	// Start two concurrent publishers along with 5 consumers
	startPublisher(t, ip, port, topicName, "p1", 100)
	startPublisher(t, ip, port, topicName, "p2", 100)

	subCtx, cancel := context.WithCancel(context.Background())

	for i := 0; i < 5; i++ {
		subId := subs[i]
		startConsumer(subCtx, t, ip, port, topicName, cgId, subId, messagesChan)
	}

	// Wait until we receive all 200 messages or timeout after 15 seconds on slow machines
	receivedMessages := make(map[string]bool)
	timeout := time.After(20 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for len(receivedMessages) < 200 {
		select {
		case msg := <-messagesChan:
			receivedMessages[msg] = true
		case <-ticker.C:
			continue
		case <-timeout:
			t.Fatalf("Timeout waiting for all messages. Received %d out of 200 messages", len(receivedMessages))
		}
	}

	cancel()

	// Verify we received all expected messages
	expectedMessages := make(map[string]bool)
	for i := 0; i < 100; i++ {
		expectedMessages["p1-meow"+strconv.Itoa(i)] = true
		expectedMessages["p2-meow"+strconv.Itoa(i)] = true
	}
	for msg := range expectedMessages {
		if !receivedMessages[msg] {
			t.Fatalf("Missing message: %s", msg)
		}
	}
	for msg := range receivedMessages {
		if !expectedMessages[msg] {
			t.Fatalf("Unexpected message: %s", msg)
		}
	}
	if len(receivedMessages) != len(expectedMessages) {
		t.Fatalf("Expected %d messages, received %d", len(expectedMessages), len(receivedMessages))
	}

	fmt.Println("Passed test 2.2")

	// ========== 2 CG (2, 4), 5 partitions ===========
	brokerCtrl3 := newBrokerController(t, &newBrokerControllerOpts{
		groupSize:        2,
		numPartitions:    5,
		maxPartitionSize: 1000,
		TTLMs:            99999,
		SweepInterval:    99,
	})

	tokens = strings.Split(brokerCtrl3.brokerAddr, ":")
	ip = tokens[0]
	port, _ = strconv.Atoi(tokens[1])

	// create a topic
	createTopic(t, ip, port, topicName)

	// create 2 consumer groups
	cgId1 := createConsumerGroup(t, ip, port, topicName)
	cgId2 := createConsumerGroup(t, ip, port, topicName)

	// now we need to add 2 subscribers to CG1, and 4 to CG2
	p, q := 2, 4
	subs1 := make([]string, p)
	subs2 := make([]string, q)
	for i := 0; i < p; i++ {
		subs1[i] = createSubscriber(t, ip, port, topicName, cgId1)
	}
	for i := 0; i < q; i++ {
		subs2[i] = createSubscriber(t, ip, port, topicName, cgId2)
	}

	// Create channels for each consumer group
	cg1Chan := make(chan string, 100)
	cg2Chan := make(chan string, 100)

	// Start both consumer groups
	subCtx1, cancel1 := context.WithCancel(context.Background())
	subCtx2, cancel2 := context.WithCancel(context.Background())
	startConsumerGroup(subCtx1, t, ip, port, topicName, cgId1, subs1, cg1Chan)
	startConsumerGroup(subCtx2, t, ip, port, topicName, cgId2, subs2, cg2Chan)

	// publish 100 messages
	n = 100
	for i := 0; i < n; i++ {
		publishMessage(t, ip, port, topicName, "", "meow"+strconv.Itoa(i))
	}

	// Wait until both consumer groups receive all messages or timeout after 15 seconds on slow machines
	timeout = time.After(15 * time.Second)
	ticker = time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	cg1Messages := make(map[string]bool)
	cg2Messages := make(map[string]bool)

	for len(cg1Messages) < n || len(cg2Messages) < n {
		select {
		case msg := <-cg1Chan:
			cg1Messages[msg] = true
		case msg := <-cg2Chan:
			cg2Messages[msg] = true
		case <-ticker.C:
			continue
		case <-timeout:
			t.Fatalf("Timeout waiting for all messages. CG1 received %d, CG2 received %d out of %d messages",
				len(cg1Messages), len(cg2Messages), n)
		}
	}

	cancel1()
	cancel2()

	// Verify both consumer groups received all messages
	expectedMessages = make(map[string]bool)
	for i := 0; i < n; i++ {
		expectedMessages["meow"+strconv.Itoa(i)] = true
	}
	for msg := range expectedMessages {
		if !cg1Messages[msg] {
			t.Fatalf("CG1 missing message: %s", msg)
		}
		if !cg2Messages[msg] {
			t.Fatalf("CG2 missing message: %s", msg)
		}
	}
	if len(cg1Messages) != len(expectedMessages) {
		t.Fatalf("CG1 expected %d messages, received %d", len(expectedMessages), len(cg1Messages))
	}
	if len(cg2Messages) != len(expectedMessages) {
		t.Fatalf("CG2 expected %d messages, received %d", len(expectedMessages), len(cg2Messages))
	}

	fmt.Println("Passed test 2.3")
}

func TestFinal_EvictionPolicy(t *testing.T) {
	/*
		PLAN:
			- when no subscribers, no messages
			- check for too many messages get evicted
			- check that messages get evicted when they expire
	*/
	topicName := "cats"
	sweepInterval := 5

	brokerCtrl0 := newBrokerController(t, &newBrokerControllerOpts{
		groupSize:        1,
		numPartitions:    1,
		maxPartitionSize: 50,
		TTLMs:            2000,
		SweepInterval:    sweepInterval,
	})

	tokens := strings.Split(brokerCtrl0.brokerAddr, ":")
	ip := tokens[0]
	port, _ := strconv.Atoi(tokens[1])

	// create a topic
	createTopic(t, ip, port, topicName)

	// publish 10 messages, should not persist
	n := 10
	for i := 0; i < n; i++ {
		publishMessage(t, ip, port, topicName, "", "meow"+strconv.Itoa(i))
	}

	// create a consumer group
	cgId := createConsumerGroup(t, ip, port, topicName)

	// create a subscriber
	subId := createSubscriber(t, ip, port, topicName, cgId)

	// read the messages
	messages := readMessage(t, ip, port, topicName, cgId, subId)

	// verify
	if len(messages) != 0 {
		t.Fatalf("Expected 0 messages, got %v", len(messages))
	}

	fmt.Println("Passed test 3.0")

	// publish 10 messages, should persist
	n = 10
	for i := 0; i < n; i++ {
		publishMessage(t, ip, port, topicName, "", "meow"+strconv.Itoa(i))
	}

	// now publish 1 more, evict meow0
	publishMessage(t, ip, port, topicName, "", "meow")

	messages = readMessage(t, ip, port, topicName, cgId, subId)

	// verify
	if len(messages) != n {
		t.Fatalf("Expected %v messages, got %v", n, len(messages))
	}

	for i := range messages {
		if i == 9 {
			break
		}
		if messages[i] != "meow"+strconv.Itoa(i+1) {
			t.Fatalf("Expected message %v to be %v, got %v", i, "meow"+strconv.Itoa(i+1), messages[i])
		}
	}
	if messages[9] != "meow" {
		t.Fatalf("Expected message %v to be %v, got %v", 9, "meow", messages[9])
	}

	fmt.Println("Passed test 3.1")

	// publish 10 messages, should persist
	for i := 0; i < n; i++ {
		publishMessage(t, ip, port, topicName, "", "meow"+strconv.Itoa(i))
	}

	// now if we wait for sweepInterval seconds, the messages should be evicted
	time.Sleep(time.Duration(sweepInterval+1) * time.Second)

	// read the messages
	messages = readMessage(t, ip, port, topicName, cgId, subId)

	// verify
	if len(messages) != 0 {
		t.Fatalf("Expected 0 messages, got %v", len(messages))
	}

	fmt.Println("Passed test 3.2")
}

func TestFinal_ReplicationFaultTolerance(t *testing.T) {
	// t.Errorf("Test 4 Not implemented")
}

func TestFinal_KeyPartitionAssignmentConsistency(t *testing.T) {
	/*
		PLAN:
			- 1 CG, 1 sub, 1 partition. Ensure all messages read with key
			- 1 CG, 1 sub, 2 partition. Ensure all messages read with key
			- 1 CG, 5 subs, 5 partitions. Ensure subscriber i only reads from partition key=i
			- 2 CG (1, 2), 2 partitions. Ensure CG1 gets both keys, CG2 sub_i gets key=i
	*/
	topicName := "cats"

	// ========== 1 CG, 1 sub, 1 partition ==========
	brokerCtrl0 := newBrokerController(t, &newBrokerControllerOpts{
		groupSize:        1,
		numPartitions:    1,
		maxPartitionSize: 1000,
		TTLMs:            99999,
		SweepInterval:    99,
	})

	tokens := strings.Split(brokerCtrl0.brokerAddr, ":")
	ip := tokens[0]
	port, _ := strconv.Atoi(tokens[1])

	// create a topic
	createTopic(t, ip, port, topicName)

	// create a consumer group
	cgId := createConsumerGroup(t, ip, port, topicName)

	// create a subscriber
	subId := createSubscriber(t, ip, port, topicName, cgId)

	// publish 10 messages to topic
	n := 10
	for i := 0; i < n; i++ {
		publishMessage(t, ip, port, topicName, "key1", "meow"+strconv.Itoa(i))
	}

	// read the messages
	messages := readMessage(t, ip, port, topicName, cgId, subId)

	// verify
	if len(messages) != n {
		t.Fatalf("Expected %v messages, got %v", n, len(messages))
	}
	for i := range messages {
		if messages[i] != "meow"+strconv.Itoa(i) {
			t.Fatalf("Expected message %v to be %v, got %v", i, "meow"+strconv.Itoa(i), messages[i])
		}
	}
	fmt.Println("Passed test 5.0")

	// ========== 1 CG, 1 sub, 2 partition ==========
	brokerCtrl1 := newBrokerController(t, &newBrokerControllerOpts{
		groupSize:        1,
		numPartitions:    2,
		maxPartitionSize: 1000,
		TTLMs:            99999,
		SweepInterval:    99,
	})

	tokens = strings.Split(brokerCtrl1.brokerAddr, ":")
	ip = tokens[0]
	port, _ = strconv.Atoi(tokens[1])

	// create a topic
	createTopic(t, ip, port, topicName)

	// create a consumer group
	cgId = createConsumerGroup(t, ip, port, topicName)

	// create a subscriber
	subId = createSubscriber(t, ip, port, topicName, cgId)

	// janky ass way to ensure they each go to a different partition:
	key1 := "key1"
	key2 := "key2"
	hash1 := HashToInt(key1) % 2
	hash2 := HashToInt(key2) % 2
	if hash1 == hash2 {
		t.Fatalf("Expected different hashes for key1 and key2")
	}

	// publish 20 messages to topic, for each partition, 10 messages where
	// index i should contain meow{i}
	n = 10
	for i := 0; i < n; i++ {
		publishMessage(t, ip, port, topicName, key1, "meow"+strconv.Itoa(i))
		publishMessage(t, ip, port, topicName, key2, "meow"+strconv.Itoa(i))
	}

	// read the messages
	messages = readMessage(t, ip, port, topicName, cgId, subId)

	// verify
	if len(messages) != (2 * n) {
		t.Fatalf("Expected %v messages, got %v", 2*n, len(messages))
	}

	// should get 20 messages in a round robin fashion from the 2 partitions
	j := 0
	for i := 0; i < (2 * n); i += 2 {
		if messages[i] != "meow"+strconv.Itoa(j) && messages[i+1] != "meow"+strconv.Itoa(j) {
			t.Fatalf("Expected message %v to be %v, got %v", i, "meow"+strconv.Itoa(j), messages[i])
		}
		j++
	}
	fmt.Println("Passed test 5.1")

	// ========== 1 CG, 5 subs, 5 partitions ==========
	brokerCtrl2 := newBrokerController(t, &newBrokerControllerOpts{
		groupSize:        1,
		numPartitions:    5,
		maxPartitionSize: 1000,
		TTLMs:            99999,
		SweepInterval:    99,
	})

	tokens = strings.Split(brokerCtrl2.brokerAddr, ":")
	ip = tokens[0]
	port, _ = strconv.Atoi(tokens[1])

	// create a topic
	createTopic(t, ip, port, topicName)

	// create a consumer group
	cgId = createConsumerGroup(t, ip, port, topicName)

	// create 5 subscribers
	subs := make([]string, 5)
	for i := 0; i < 5; i++ {
		subs[i] = createSubscriber(t, ip, port, topicName, cgId)
	}

	// here we will publish 25 messages, but this time we will distribute them randomly
	keys := []string{"bruh", "kms", "help", "key4", "how"}
	for i, key := range keys {
		hash := HashToInt(key) % 5
		if hash != i {
			t.Fatalf("Expected hash for %q to be %d, got %d", key, i, hash)
		}
	}

	// leetcode time baby
	n = 25
	freqCount := make(map[string]int)
	for i := 0; n < 25; i++ {
		randIdx := rand.Intn(len(keys))
		publishMessage(t, ip, port, topicName, keys[randIdx], "meow"+strconv.Itoa(i))
		freqCount[keys[randIdx]]++
	}

	messages = []string{}

	// now make sure that the messages arrive from the partition that they are supposed to
	for i, subId := range subs {
		subset := readMessage(t, ip, port, topicName, cgId, subId)
		if len(subset) != freqCount[keys[i]] {
			t.Fatalf("Expected %v messages, got %v", freqCount[keys[i]], len(subset))
		}
		messages = append(messages, subset...)
	}

	// ========== 2 CG (1, 2), 2 partitions ==========
	brokerCtrl3 := newBrokerController(t, &newBrokerControllerOpts{
		groupSize:        2,
		numPartitions:    2,
		maxPartitionSize: 1000,
		TTLMs:            99999,
		SweepInterval:    99,
	})

	tokens = strings.Split(brokerCtrl3.brokerAddr, ":")
	ip = tokens[0]
	port, _ = strconv.Atoi(tokens[1])

	// create a topic
	createTopic(t, ip, port, topicName)

	// create 2 consumer groups
	cgId1 := createConsumerGroup(t, ip, port, topicName)
	cgId2 := createConsumerGroup(t, ip, port, topicName)

	// CG1 gets 1 sub, CG2 gets 2 subs
	subs1 := make([]string, 1)
	subs2 := make([]string, 2)
	for i := 0; i < 1; i++ {
		subs1[i] = createSubscriber(t, ip, port, topicName, cgId1)
	}
	for i := 0; i < 2; i++ {
		subs2[i] = createSubscriber(t, ip, port, topicName, cgId2)
	}

	// publish 20 messages to topic
	n = 10
	for i := 0; i < n; i++ {
		publishMessage(t, ip, port, topicName, "key1", "meow"+strconv.Itoa(i))
		publishMessage(t, ip, port, topicName, "key2", "meow"+strconv.Itoa(i))
	}

	// CG1 should get 20 messages
	messages = readMessage(t, ip, port, topicName, cgId1, subs1[0])
	if len(messages) != (2 * n) {
		t.Fatalf("Expected %v messages, got %v", n, len(messages))
	}

	// CG2 should get 20 messages
	messages = []string{}
	for i := 0; i < 2; i++ {
		subset := readMessage(t, ip, port, topicName, cgId2, subs2[i])
		if len(subset) != n {
			t.Fatalf("Expected %v messages, got %v", n, len(subset))
		}
		messages = append(messages, subset...)
	}
	if len(messages) != (2 * n) {
		t.Fatalf("Expected %v messages, got %v", 2*n, len(messages))
	}
}

func TestFinal_BrokerLoadBalancing(t *testing.T) {
	// t.Errorf("Test 6 Not implemented")
}

func TestFinal_OverlappingTopicsAndHierachicalReads(t *testing.T) {
	// t.Errorf("Test 7 Not implemented")
}
