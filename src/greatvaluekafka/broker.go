package greatvaluekafka

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/rs/zerolog/log"
)

type TopicCreateRequest struct {
	// the topic name
	Name string `json:"name"`
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
	Activate   func() RemoteError
	Deactivate func() RemoteError
	Terminate  func() RemoteError
	GetStatus  func() (BrokerStatusReport, RemoteError)
}

// an instance of a kafka broker.
// each broker can have multiple topics.
type Broker struct {
	// the broker's participant index
	brokerIndex int

	httpServer   *http.Server
	shouldListen bool

	// the broker's topics
	Topics sync.Map
}

type BrokerOpts struct {
	BrokerIndex int
	BrokerAddr  string
}

// NewBroker creates a new broker
func NewBroker(bOpts *BrokerOpts) *Broker {
	b := &Broker{
		brokerIndex: bOpts.BrokerIndex,
		Topics:      sync.Map{},
	}

	// create multiplexer
	mux := http.NewServeMux()

	// Register handlers
	mux.HandleFunc("/topics", b.handleTopicCreate)

	b.httpServer = &http.Server{
		Addr:    bOpts.BrokerAddr,
		Handler: b.activeMiddleware(mux),
	}

	return b
}

// Activate starts the broker server
func (b *Broker) Activate() RemoteError {
	// start the http server where the broker listens for publisher/subscriber requests
	go func() {
		if err := b.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Broker %v HTTP Server error: %v\n", b.brokerIndex, err)
		}
	}()

	b.shouldListen = true

	// TODO: activate the Topic, aka it's raft groups per partition
	return RemoteError{}
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

	log.Printf("Received topic creation request: %v", req)

	// check if the topic already exists
	if _, ok := b.Topics.Load(req.Name); ok {
		w.WriteHeader(http.StatusOK)
		// TODO: return all relevant topic metadata
		w.Write([]byte(req.Name))
		return
	}

	// create the topic
	topic := NewTopic(req.Name, NUM_PARTITIONS)
	b.Topics.Store(req.Name, &topic)
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(topic.Name))
}
