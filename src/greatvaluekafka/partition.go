package greatvaluekafka

import "time"
import "fmt"

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
	queue []*PartitionItem

	// metadata per partition
	size int

	// The logical start of the queue
	head int
}

// NewPartition creates a new partition
func NewPartition() *Partition {
	return &Partition{
		queue: make([]*PartitionItem, 0),
		size:  0,
		head:  0,
	}
}

func (p *Partition) Dequeue(subs []*Subscriber) {
	// check if the queue is empty
	if len(p.queue) == 0 {
		return
	}

	// find out the minimum index all subscribers are at
	minIndex := subs[0].ReadIndex
	for i := 1; i < len(subs); i++ {
		if subs[i].ReadIndex < minIndex {
			minIndex = subs[i].ReadIndex
		}
	}

	// remove until the minimum index
	for i := 0; i < minIndex; i++ {
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
	// add item to the queue
	p.queue = append(p.queue, item)

	// update the size of the partition
	p.size += item.size

	// Check if the max queue size is reached
	for p.size > MAX_PARTITION_SIZE {
		// Remove the oldest item from the queue
		p.queue[0] = nil
		p.queue = p.queue[1:]
		p.size -= item.size
		p.head++
	}
}

func (p *Partition) ReadBySub(sub *Subscriber) *PartitionItem {
	realIndex := sub.ReadIndex - p.head

	realIndex = max(realIndex, 0)

	if realIndex >= len(p.queue) {
		return nil
	}

	fmt.Printf("length of queue: %d\n", len(p.queue))
	fmt.Printf("read index: %d\n", sub.ReadIndex)
	fmt.Printf("real index: %d\n", realIndex)
	fmt.Printf("head: %d\n", p.head)

	item := p.queue[realIndex]

	// update the read index of the subscriber
	// TODO: Does this cover all the edge cases?
	sub.ReadIndex = p.head + realIndex + 1

	return item
}
