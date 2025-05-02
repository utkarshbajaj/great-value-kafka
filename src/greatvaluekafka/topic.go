package greatvaluekafka

import (
	"math/rand"
	"sync"
)

type Topic struct {
	// topic name
	Name string

	// topic partitions
	Partitions []*Partition

	// topic consumer groups
	ConsumerGroups sync.Map // map[string]*ConsumerGroup
}

type TopicOpts struct {
	Name             string
	Partitions       int
	MaxPartitionSize int
	TTLMs            int
	SweepInterval    int
}

// NewTopic creates a new topic with the given name and partitions
func NewTopic(tOpts *TopicOpts) *Topic {
	// create the topic
	topic := &Topic{
		Name:           tOpts.Name,
		Partitions:     make([]*Partition, tOpts.Partitions),
		ConsumerGroups: sync.Map{},
	}

	// create the partitions
	for i := range tOpts.Partitions {
		subscribers := []*Subscriber{}
		pOpts := &partitionOpts{
			maxSize:       tOpts.MaxPartitionSize,
			PartitionId:   i,
			ttlMs:         tOpts.TTLMs,
			subscribers:   &subscribers,
			sweepInterval: tOpts.SweepInterval,
		}
		topic.Partitions[i] = NewPartition(pOpts)
	}

	return topic
}

func (t *Topic) AddConsumerGroup(cgId string, consumerGroup *ConsumerGroup) {
	t.ConsumerGroups.Store(cgId, consumerGroup)
	for _, partition := range t.Partitions {
		// each partition should have a pointer to the consumer group subscribers
		partition.subscribers = consumerGroup.Subscribers
	}
}

// ReadBySub reads a batch of messages from this topics
func (t *Topic) ReadBySub(sub *Subscriber) []string {
	// Loop through the partitions and dequeue the items
	itemsFetched := 0
	batch := make([]string, 0)

	for itemsFetched < MAX_POLL_RECORDS {
		foundItem := false

		for j := range t.Partitions {
			if !sub.ShouldReadPartition[j] {
				continue
			}

			// dequeue the items from the partition
			item := t.Partitions[j].ReadBySub(sub)
			if item == nil {
				continue
			}

			foundItem = true

			batch = append(batch, string(item.Message))
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

// PushToPartition pushes a message to a partition
// If the key is empty, a random partition is selected
// Otherwise, the partition is selected based on the key
func (t *Topic) PushToPartition(message []byte, key string) {
	if key == "" {
		partitionIndex := rand.Intn(len(t.Partitions))
		t.Partitions[partitionIndex].Enqueue(NewPartitionItem(message))
	} else {
		// Hash the key to get the partition index
		partitionIndex := HashToInt(key) % len(t.Partitions)
		t.Partitions[partitionIndex].Enqueue(NewPartitionItem(message))
	}
}
