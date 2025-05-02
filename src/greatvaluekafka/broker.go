package greatvaluekafka

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type TopicCreateRequest struct {
	// the topic name
	Name string `json:"name"`
}

type TopicPublishRequest struct {
	Key     string `json:"key"`
	Message string `json:"message"`
}

type BrokerStatusReport struct {
	Active      bool
	BrokerIndex int
	// TODO: answer questions dynamically when we generate report.
	// - for topic i:
	//	 - for each partition j:
	// 		 - who is the leader?
}

type BrokerControlInterface struct {
	Activate   func() error
	Deactivate func() error
	Terminate  func() error
	GetStatus  func() (BrokerStatusReport, error)
}

// an instance of a kafka broker.
// each broker can have multiple topics.
type Broker struct {
	// the broker's participant index
	brokerIndex int

	httpServer     *http.Server
	shouldListen   bool
	controllerStub *rpc.Server

	// number of partitions per Topics
	numPartitions    int
	maxPartitionSize int

	// the http listener for the rpc server
	rpcListener net.Listener

	// the broker's topics
	Topics sync.Map
}

// BrokerOpts are the options for creating a new broker
type BrokerOpts struct {
	BrokerIndex int

	// number of partitions per Topics
	NumPartitions int

	// max size in bytes for a partition
	MaxPartitionSize int

	// the address that pub/sub clients connect to
	BrokerAddr string

	// the address that listens to the controller
	ControlAddr string

	// rpc path
	RPCPath string

	// debug path
	DebugPath string
}

// NewBroker creates a new broker
func NewBroker(bOpts *BrokerOpts) *Broker {
	b := &Broker{
		brokerIndex:      bOpts.BrokerIndex,
		Topics:           sync.Map{},
		controllerStub:   rpc.NewServer(),
		numPartitions:    bOpts.NumPartitions,
		maxPartitionSize: bOpts.MaxPartitionSize,
	}

	// Make sure this does not cause a deadlock
	brokerRPC := &BrokerRPC{
		Broker: b,
	}

	b.controllerStub.Register(brokerRPC)
	b.controllerStub.HandleHTTP(bOpts.RPCPath, bOpts.DebugPath)

	var err error
	b.rpcListener, err = net.Listen("tcp", bOpts.ControlAddr)
	if err != nil {
		log.Fatal().Err(err).Msgf("Failed to listen on %s", bOpts.ControlAddr)
	}
	go http.Serve(b.rpcListener, nil)

	// create multiplexer
	mux := http.NewServeMux()

	// Register handlers
	mux.HandleFunc("/topics", b.handleTopicCreate)
	mux.HandleFunc("/topics/{name}/subscribe", b.handleTopicSubscribe)
	mux.HandleFunc("/topics/{name}/consumer-groups/{cgid}/subscribers/{sid}", b.handleTopicConsume)
	mux.HandleFunc("/topics/{name}/publish", b.handleTopicPublish)
	mux.HandleFunc("/topics/{name}/consumer-groups/{id}/subscribe", b.handleConsumerGroupSubscribe)

	b.httpServer = &http.Server{
		Addr:    bOpts.BrokerAddr,
		Handler: b.activeMiddleware(mux),
	}

	return b
}

// Activate starts the broker server
func (b *Broker) Activate() error {
	log.Info().Msgf("Activating broker %v on %v", b.brokerIndex, b.httpServer.Addr)
	// start the http server where the broker listens for publisher/subscriber requests
	go func() {
		if err := b.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Broker %v HTTP Server error: %v\n", b.brokerIndex, err)
		}
	}()

	b.shouldListen = true

	// TODO: activate the Topic, aka it's raft groups per partition
	return nil
}

func (b *Broker) Deactivate() error {
	log.Info().Msgf("Deactivating broker %v on %v", b.brokerIndex, b.httpServer.Addr)
	b.shouldListen = false

	// TODO: deactivate the Topic, aka it's raft groups per partition
	return nil
}

func (b *Broker) Terminate() error {
	b.shouldListen = false
	b.httpServer.Close()

	// kill the rpc httpServer
	b.rpcListener.Close()

	return nil
}

func (b *Broker) GetStatus() (BrokerStatusReport, error) {
	return BrokerStatusReport{
		Active:      b.shouldListen,
		BrokerIndex: b.brokerIndex,
	}, nil
}

// activeMiddleware serves as middleware for the http server; it is purely
// so that we dont have to actually shut down the server. we "pause" it.
func (b *Broker) activeMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !b.shouldListen {
			http.Error(w, "Service temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// handleTopicCreate handles a topic creation request, if the topic exists already
// then it will soft fail (nothing happens)
func (b *Broker) handleTopicCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	var req TopicCreateRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// lowercase the topic name
	req.Name = strings.ToLower(req.Name)

	log.Printf("Received topic creation request: %v", req)

	// check if the topic already exists
	if _, ok := b.Topics.Load(req.Name); ok {
		w.WriteHeader(http.StatusOK)
		// TODO: return all relevant topic metadata
		w.Write([]byte(req.Name))
		return
	}

	// create the topic
	topicOpts := &TopicOpts{
		Name:             req.Name,
		Partitions:       b.numPartitions,
		MaxPartitionSize: b.maxPartitionSize,
	}
	topic := NewTopic(topicOpts)
	b.Topics.Store(req.Name, topic)
	log.Printf("Created topic %v", topic.Name)
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(topic.Name))
}

// handleTopicSubscribe creates a new consumer group for the topic, then
// returns the consumer group id
func (b *Broker) handleTopicSubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	tokens := strings.Split(r.URL.Path, "/")

	if len(tokens) != 4 {
		http.Error(w, "Invalid URL", http.StatusBadRequest)
		return
	}

	// parse the topic name from the URL
	topicName := strings.ToLower(tokens[2])

	if topicName == "" {
		http.Error(w, "Invalid topic name", http.StatusBadRequest)
		return
	}

	log.Printf("Received topic subscription request: %v", topicName)

	// check if the topic exists
	topic, ok := b.Topics.Load(topicName)
	if !ok {
		http.Error(w, "Topic not found for "+topicName, http.StatusNotFound)
		return
	}

	// Typecase the topic to a *Topic
	topicPtr := topic.(*Topic)

	// create a new consumer group
	consumerGroupId := uuid.New()
	consumerGroupPtr := NewConsumerGroup(consumerGroupId.String())

	// add the consumer group to the topic
	topicPtr.ConsumerGroups.Store(consumerGroupId.String(), consumerGroupPtr)

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(consumerGroupId.String()))
}

// handleTopicConsume handles a read request for a topic
func (b *Broker) handleTopicConsume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// parse the topic name and the sub id from the URL
	// the URL should be in the format /subscribers/{id}/topics/{name}
	tokens := strings.Split(r.URL.Path, "/")

	if len(tokens) != 7 {
		http.Error(w, "Invalid URL", http.StatusBadRequest)
		return
	}

	// parse the topic name from the URL
	topicName := strings.ToLower(tokens[2])
	if topicName == "" {
		http.Error(w, "Invalid topic name", http.StatusBadRequest)
		return
	}

	// make sure the topic exists
	topic, ok := b.Topics.Load(topicName)
	if !ok {
		http.Error(w, "Topic not found", http.StatusNotFound)
		return
	}

	topicPtr := topic.(*Topic)

	// parse the consumer group id from the URL
	cgId := tokens[4]
	if cgId == "" {
		http.Error(w, "Invalid consumer group id", http.StatusBadRequest)
		return
	}

	// Check if the consumer group exists
	consumerGroup, ok := topicPtr.ConsumerGroups.Load(cgId)
	if !ok {
		http.Error(w, "Consumer group not found for "+cgId, http.StatusNotFound)
		return
	}

	consumerGroupPtr := consumerGroup.(*ConsumerGroup)

	// parse the subscriber id from the URL
	subscriberId, err := uuid.Parse(tokens[6])
	if err != nil {
		http.Error(w, "Invalid subscriber id, must be a valid UUID", http.StatusBadRequest)
		return
	}

	subIndex := consumerGroupPtr.SubscriberIndex[subscriberId]
	subscriberPtr := consumerGroupPtr.Subscribers[subIndex]

	// read the topic for the subscriber
	items := topicPtr.ReadBySub(subscriberPtr)

	// return the items to the subscriber in a json array
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(items)
}

// handleTopicPublish handles a publish request for a topic
func (b *Broker) handleTopicPublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// parse the topic name from the URL
	topicName := r.URL.Path[len("/topics/") : len(r.URL.Path)-len("/publish")]
	if topicName == "" {
		http.Error(w, "Invalid topic name", http.StatusBadRequest)
		return
	}

	log.Printf("Received topic publish request: %v", topicName)

	// check if the topic exists
	topic, ok := b.Topics.Load(topicName)
	if !ok {
		http.Error(w, "Topic not found", http.StatusNotFound)
		return
	}

	// Typecase the topic to a *Topic
	topicPtr := topic.(*Topic)

	var req TopicPublishRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// TODO: Check if there is a problem with usign a goroutine here
	// Mainly doing this to avoid blocking the http request
	go topicPtr.PushToPartition([]byte(req.Message), req.Key)

	w.WriteHeader(http.StatusAccepted)
}

// handleConsumerGroupSubscribe handles a consumer group subscription request
func (b *Broker) handleConsumerGroupSubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// parse the consumer group id from the URL
	tokens := strings.Split(r.URL.Path, "/")

	if len(tokens) != 6 {
		http.Error(w, "Invalid URL", http.StatusBadRequest)
		return
	}

	topicName := tokens[2]
	topic, ok := b.Topics.Load(topicName)
	if !ok {
		http.Error(w, "Topic not found for "+topicName, http.StatusNotFound)
		return
	}

	topicPtr := topic.(*Topic)

	cgId := tokens[4]
	if cgId == "" {
		http.Error(w, "Invalid consumer group id", http.StatusBadRequest)
		return
	}

	// Check if the consumer group exists
	consumerGroup, ok := topicPtr.ConsumerGroups.Load(cgId)
	if !ok {
		http.Error(w, "Consumer group not found for "+cgId, http.StatusNotFound)
		return
	}

	// Typecast the consumer group to a *ConsumerGroup
	consumerGroupPtr := consumerGroup.(*ConsumerGroup)

	// dont allow more subs than partitions
	if len(consumerGroupPtr.Subscribers) >= b.numPartitions {
		http.Error(w, "Max subscribers reached "+strconv.Itoa(b.numPartitions), http.StatusBadRequest)
		return
	}

	subscriber := NewSubscriber(b.numPartitions)
	consumerGroupPtr.Subscribers = append(consumerGroupPtr.Subscribers, subscriber)
	consumerGroupPtr.SubscriberIndex[subscriber.Id] = len(consumerGroupPtr.Subscribers) - 1

	// we will need to ensure that each subscriber is now remapped to new partitions
	// we decide with partitions_per_sub = partitions/subscribers
	numSubs := len(consumerGroupPtr.Subscribers)
	partitionsPerSub := b.numPartitions / numSubs

	// reset mappings for each subscriber
	for _, subscriber := range consumerGroupPtr.Subscribers {
		subscriber.ShouldReadPartition = make([]bool, b.numPartitions)
	}

	// TODO: remove log; print the numSubs, partitionsPerSub, and subscribers
	log.Printf("Subscribers: %v, partitionsPerSub: %v, numSubscribers: %v", numSubs, partitionsPerSub, numSubs)

	// remap the subscriber to the new partitions
	currSub := 0
	for i := range b.numPartitions {
		consumerGroupPtr.Subscribers[currSub].ShouldReadPartition[i] = true
		if (i+1)%partitionsPerSub == 0 {
			currSub = (currSub + 1) % numSubs
		}
	}

	// TODO: remove log; for each subscriber, should print which paritions they should read
	for i, subscriber := range consumerGroupPtr.Subscribers {
		log.Printf("%v Subscriber %v should read partitions: %v", i, subscriber.Id, subscriber.ShouldReadPartition)
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(subscriber.Id.String()))
}
