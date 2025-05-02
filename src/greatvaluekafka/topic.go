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
		pOpts := &partitionOpts{
			maxSize:     tOpts.MaxPartitionSize,
			PartitionId: i,
		}
		topic.Partitions[i] = NewPartition(pOpts)
	}

	return topic
}

// 1. How many return values per patrition? 10 for now
// 2. How do we select the partition to take out the value from? Round robin

func (t *Topic) ReadBySub(sub *Subscriber) []string {
	// log.Printf("Read request by subscriber: %v", sub.Id)
	// Loop through the partitions and dequeue the items
	itemsFetched := 0
	batch := make([]string, 0)

	for itemsFetched < MAX_POLL_RECORDS {
		foundItem := false

		for j := range t.Partitions {
			if !sub.ShouldReadPartition[j] {
				continue
			}
			// log.Printf("Reading partition %v", j)

			// dequeue the items from the partition
			item := t.Partitions[j].ReadBySub(sub)
			if item == nil {
				continue
			}

			foundItem = true

			batch = append(batch, string(item.Message))
			itemsFetched++
			// log.Printf("Found item: %v", string(item.Message))

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
