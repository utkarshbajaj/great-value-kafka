package greatvaluekafka

import "github.com/google/uuid"

type ConsumerGroup struct {
	// the consumer group id
	Id string

	// the consumer group subscribers
	Subscribers []*Subscriber

	// map from uuid to the index in array above
	SubscriberIndex map[uuid.UUID]int

	// if the consumer group has dependent consumer groups
	DependentConsumerGroups []*ConsumerGroup

	// the subscribers a subscriber is parent to in this consumer group
	DependentSubscribers map[string][]string

	// map from the subscriber id to the index in the array above
	// this is used to find the consumer group index for a subscriber
	DependentSubscriberIndex map[string]int
}

func NewConsumerGroup(id string) *ConsumerGroup {
	return &ConsumerGroup{
		Id:                       id,
		SubscriberIndex:          make(map[uuid.UUID]int),
		DependentSubscribers:     make(map[string][]string),
		DependentSubscriberIndex: make(map[string]int),
	}
}

