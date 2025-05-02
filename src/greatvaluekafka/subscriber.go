package greatvaluekafka

import (
	"sync"

	"github.com/google/uuid"
)

type Subscriber struct {
	// the subscriber's id
	Id uuid.UUID

	// if the subscribers reads this partition or nor
	ShouldReadPartition []bool
	
	// the index of the last read item per partition
	ReadIndex []int

	// read index lock
	subMtx sync.RWMutex
}

func NewSubscriber(partitionCount int) *Subscriber {
	return &Subscriber{
		Id:        uuid.New(),
		ShouldReadPartition: make([]bool, partitionCount),
		ReadIndex: make([]int, partitionCount),
	}
}
