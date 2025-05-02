package greatvaluekafka

import "github.com/google/uuid"

type ConsumerGroup struct {
	// the consumer group id
	Id string

	// the consumer group subscribers
	Subscribers *[]*Subscriber

	// map from uuid to the index in array above
	SubscriberIndex map[uuid.UUID]int

	// are we handling the topics per consumer group?
	// the consumer group topics
	// Topics []*Topic
}

func NewConsumerGroup(id string) *ConsumerGroup {
	subscribers := []*Subscriber{}
	return &ConsumerGroup{
		Id:              id,
		Subscribers:     &subscribers,
		SubscriberIndex: make(map[uuid.UUID]int),
	}
}
