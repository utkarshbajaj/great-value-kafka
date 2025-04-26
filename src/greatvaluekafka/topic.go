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

	pOpts := &partitionOpts{
		maxSize: MAX_PARTITION_SIZE,
	}

	// create the partitions
	for i := range partitions {
		topic.Partitions[i] = NewPartition(pOpts)
	}

	return topic
}

// TODO: Change this to a different data structure if we want to support unsubscribing

// Subscribe adds a subscriber to the topic
func (t *Topic) Subscribe(sub *Subscriber) int {
	// add the subscriber to the topic
	t.Subscribers = append(t.Subscribers, sub)

	// return the index of the Subscribers
	return len(t.Subscribers) - 1
}

// 1. How many return values per patrition? 10 for now
// 2. How do we select the partition to take out the value from? Round robin

func (t *Topic) ReadBySub(sub *Subscriber) [][]byte {
	// Loop through the partitions and dequeue the items
	itemsFetched := 0
	batch := make([][]byte, 0)

	for itemsFetched < MAX_POLL_RECORDS {
		foundItem := false

		for j := range t.Partitions {
			// dequeue the items from the partition
			item := t.Partitions[j].ReadBySub(sub)
			if item == nil {
				continue
			}

			foundItem = true

			batch = append(batch, item.Message)
			itemsFetched++

			if itemsFetched >= MAX_POLL_RECORDS {
				break
			}
		}

		if !foundItem {
			break
		}
	}

	return batch
}
