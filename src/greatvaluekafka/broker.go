package greatvaluekafka

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
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

	// the broker's topics
	Topics sync.Map
}

type BrokerOpts struct {
	BrokerIndex int

	// the address that pub/sub clients connect to
	BrokerAddr string

	// the address that listens to the controller
	ControlAddr string
}

// NewBroker creates a new broker
func NewBroker(bOpts *BrokerOpts) *Broker {
	b := &Broker{
		brokerIndex:    bOpts.BrokerIndex,
		Topics:         sync.Map{},
		controllerStub: rpc.NewServer(),
	}

	brokerRPC := &BrokerRPC{
		Broker: b,
	}

	b.controllerStub.Register(brokerRPC)
	b.controllerStub.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	l, err := net.Listen("tcp", bOpts.ControlAddr)
	if err != nil {
		log.Fatal().Err(err).Msgf("Failed to listen on %s", bOpts.ControlAddr)
	}
	go http.Serve(l, nil)

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
