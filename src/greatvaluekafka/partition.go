package greatvaluekafka

import (
	"github.com/rs/zerolog/log"
	"time"
)

// This is our queue message
type PartitionItem struct {
	// need to store data
	Message []byte

	// need to store metadata
	createdAt time.Time
	size      int
}

// NewPartitionItem creates a new PartitionItem
// with the given message and sets the createdAt time
func NewPartitionItem(message []byte) *PartitionItem {
	return &PartitionItem{
		Message:   message,
		createdAt: time.Now(),
		size:      len(message),
	}
}

// Patrition is the actual message queue
type Partition struct {
	// the partition id
	Id int

	// the partition message queue
	queue []*PartitionItem

	// metadata per partition
	size int

	// The logical start of the queue
	head int

	// the maximum size of the partition
	partitionLimit int
}

type partitionOpts struct {
	// the maximum size of the partition
	maxSize     int
	PartitionId int
}

// NewPartition creates a new partition
func NewPartition(opts *partitionOpts) *Partition {
	return &Partition{
		Id:             opts.PartitionId,
		queue:          make([]*PartitionItem, 0),
		size:           0,
		head:           0,
		partitionLimit: opts.maxSize,
	}
}

func (p *Partition) Dequeue(subs []*Subscriber) {
	// check if the queue is empty
	if len(p.queue) == 0 {
		return
	}

	// find out the minimum index all subscribers are at
	minIndex := subs[0].ReadIndex[p.Id]
	for i := 1; i < len(subs); i++ {
		if subs[i].ReadIndex[p.Id] < minIndex {
			minIndex = subs[i].ReadIndex[p.Id]
		}
	}

	// remove until the minimum index
	for range minIndex {
		// remove the first item from the queue
		item := p.queue[0]

		// this should be gargbage collected
		p.queue[0] = nil

		p.queue = p.queue[1:]
		p.size -= item.size
	}

	// update the head of the queue
	p.head = minIndex
}

func (p *Partition) Enqueue(item *PartitionItem) {
	log.Printf("Enqueueing item: %v", item.Message)

	// add item to the queue
	p.queue = append(p.queue, item)

	// update the size of the partition
	p.size += item.size

	// Check if the max queue size is reached
	for p.size > p.partitionLimit {
		// Remove the oldest item from the queue
		p.queue[0] = nil
		p.queue = p.queue[1:]
		p.size -= item.size
		p.head++
	}
}

func (p *Partition) ReadBySub(sub *Subscriber) *PartitionItem {
	realIndex := max(sub.ReadIndex[p.Id]-p.head, 0)
	log.Printf("Reading item at index: %v", realIndex)

	if realIndex >= len(p.queue) {
		return nil
	}

	item := p.queue[realIndex]

	// update the read index of the subscriber
	// TODO: Does this cover all the edge cases?
	sub.ReadIndex[p.Id] = p.head + realIndex + 1

	log.Printf("Read index for subscriber %v is now %v", sub.Id, sub.ReadIndex[p.Id])

	return item
}
