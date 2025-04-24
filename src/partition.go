package greatvaluekafka

import "time"

// This is our queue message
type PartitionItem struct {
	// need to store data
	message []byte

	// need to store metadata
	createdAt time.Time
	size      int
}

// this is the message queue
type Partition struct {
	queue []*PartitionItem

	// metadata per partition
	size int
}
