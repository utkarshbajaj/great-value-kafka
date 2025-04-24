package greatvaluekafka

import "time"

type PartitionItem struct {
	// need to store data
	data []byte

	// need to store metadata
	createdAt time.Time
	size      int
}

type Partition struct {
}
