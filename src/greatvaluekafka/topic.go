package greatvaluekafka

type Topic struct {
	// topic name
	Name string

	// topic partitions
	Partitions []*Partition

	// topic subscribers
	Subscribers []*Subscriber
}

// NewTopic creates a new topic with the given name and partitions
func NewTopic(name string, partitions int) *Topic {
	// create the topic
	topic := &Topic{
		Name:        name,
		Partitions:  make([]*Partition, partitions),
		Subscribers: make([]*Subscriber, 0),
	}

	// create the partitions
	for i := 0; i < partitions; i++ {
		topic.Partitions[i] = NewPartition()
	}

	return topic
}
