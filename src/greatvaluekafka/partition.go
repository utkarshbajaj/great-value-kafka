package greatvaluekafka

import (
	"sync"
	"time"

	"github.com/rs/zerolog/log"
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
	Size int

	// The logical start of the queue
	head int

	// the maximum size of the partition
	partitionLimit int

	// lock for the queue so that concurrent reads/writes are safe
	partitionLock sync.RWMutex
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
		Size:           0,
		head:           0,
		partitionLimit: opts.maxSize,
	}
}

// Dequeue only removes from the partition items that have been all read
// by the subscribers. The minIndex is found by checking which subscriber
// has the lowest progress on reading the queue, since their next read
// index should not be removed, but those before it should be.
func (p *Partition) Dequeue(subs []*Subscriber) {
	p.partitionLock.Lock()
	defer p.partitionLock.Unlock()
	// check if the queue is empty
	if len(p.queue) == 0 {
		return
	}

	// find out the minimum index all subscribers are at
	subs[0].subMtx.RLock()
	minIndex := subs[0].ReadIndex[p.Id]
	subs[0].subMtx.RUnlock()

	for i := 1; i < len(subs); i++ {
		subs[i].subMtx.RLock()
		index := subs[i].ReadIndex[p.Id]
		subs[i].subMtx.RUnlock()
		
		if index < minIndex {
			minIndex = index
		}
	}

	// remove until the minimum index
	for range minIndex {

		// remove the first item from the queue
		item := p.queue[0]

		// this should be gargbage collected
		p.queue[0] = nil

		p.queue = p.queue[1:]
		p.Size -= item.size
	}

	// update the head of the queue
	p.head = minIndex
}

// Enqueue will add the given item to the partition queue, and update the size
// of the partition. If this size exceeds the partition limit, the oldest items
// will be removed from the queue decreasing the size until its below the limit.
func (p *Partition) Enqueue(item *PartitionItem) {
	p.partitionLock.Lock()
	defer p.partitionLock.Unlock()
	log.Printf("Enqueueing item: %v", item.Message)

	// add item to the queue
	p.queue = append(p.queue, item)

	// update the size of the partition
	p.Size += item.size

	// Check if the max queue size is reached
	for p.Size > p.partitionLimit {
		// Remove the oldest item from the queue
		itemSize := p.queue[0].size
		p.queue[0] = nil
		p.queue = p.queue[1:]
		p.Size -= itemSize
		p.head++
	}
}

// ReadBySub will read as much of the next item as possible from the partition
// queue, starting from the given subscriber's read index. It will return the
// next item, or nil if there are no more items to read for this subscriber.
func (p *Partition) ReadBySub(sub *Subscriber) *PartitionItem {
	p.partitionLock.RLock()
	sub.subMtx.Lock()
	defer sub.subMtx.Unlock()
	defer p.partitionLock.RUnlock()

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
