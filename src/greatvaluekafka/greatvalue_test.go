package greatvaluekafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/rpc"
	// "sort"
	"strconv"
	// "strings"
	"testing"
)

type BrokerController struct {
	Broker     *Broker
	groupSize  int
	brokerAddr string

	rpcClient *rpc.Client
}

func newBrokerController(t *testing.T, groupSize int, numPartitions int) *BrokerController {

	// Create a random port for the broker
	port := rand.Intn(20000) + 20000

	// Create a random rpc path and debug path
	rpcPath := "/rpc" + strconv.Itoa(rand.Intn(10000))
	debugPath := "/debug" + strconv.Itoa(rand.Intn(10000))

	brokerAddr := fmt.Sprintf("127.0.0.1:%v", port)
	ctrlAddr := fmt.Sprintf("127.0.0.1:%v", port+1)

	brokerCtrl := &BrokerController{
		Broker: NewBroker(&BrokerOpts{
			BrokerIndex:   0,
			BrokerAddr:    brokerAddr,
			ControlAddr:   ctrlAddr,
			NumPartitions: numPartitions,
			RPCPath:       rpcPath,
			DebugPath:     debugPath,
		}),
		groupSize:  groupSize,
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

	fmt.Println(url)

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
	fmt.Println(statusCode)
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
	_, statusCode := sendHttpRequest(t, ip, port, url, "POST", reqBody)
	if statusCode != http.StatusAccepted {
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

/*
// TODO: Add tests for this
// Test that the endpoints are working well
// Test that you can deactivate and activate the broker and the HTTP request behave the same way
// func Test_Setup(t *testing.T) {
// 	broker := NewBroker(&BrokerOpts{
// 		BrokerIndex: 0,
// 		BrokerAddr:  "127.0.0.1:9092",
// 		ControlAddr: "127.0.0.1:9093",
// 	})
// 	go broker.Activate()
// }

// Test_SinglePublishSingleSubscribe tests a single publish and single subscribe
// with one partition. This should work for multiple messages.
func Test_SinglePublishSingleSubscribe(t *testing.T) {
	// This creates a broker controller
	// It activates the broker to receive requests from clients
	brokerCtrl := newBrokerController(t, 1, 1)

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

func Test_MultiplePublishersMultipleSubscribers(t *testing.T) {
	brokerCtrl := newBrokerController(t, 1, 2)

	tokens := strings.Split(brokerCtrl.brokerAddr, ":")
	ip := tokens[0]
	port, _ := strconv.Atoi(tokens[1])

	topicName := "cats"

	// create a topic
	createTopic(t, ip, port, topicName)

	// create a consumer group
	cgId := createConsumerGroup(t, ip, port, topicName)

	numSubscribers := 2

	// create multiple subscribers
	subs := make([]string, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		subs[i] = createSubscriber(t, ip, port, topicName, cgId)
	}

	var originalMessages []string

	// Publish 10 messages using a loop
	for i := 0; i < 10; i++ {
		message := "meow" + strconv.Itoa(i)
		originalMessages = append(originalMessages, message)
		publishMessage(t, ip, port, topicName, "", message)
	}

	var messages []string

	// read the messages
	for i := 0; i < numSubscribers; i++ {
		messages = append(messages, readMessage(t, ip, port, topicName, cgId, subs[i])...)
	}

	if len(messages) != 10 {
		t.Fatalf("Expected 10 messages, got %v", len(messages))
	}

	// check if the message slices are the same
	// ordering does not matter, so we can sort them
	sort.Strings(originalMessages)
	sort.Strings(messages)

	for i := range originalMessages {
		if originalMessages[i] != messages[i] {
			t.Fatalf("Expected message %v to be %v, got %v", i, originalMessages[i], messages[i])
		}
	}

	fmt.Println("Passed test 2")
}
*/
