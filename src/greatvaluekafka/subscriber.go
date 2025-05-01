package greatvaluekafka

import (
	"sync"

	"github.com/google/uuid"
)

type Subscriber struct {
	// the subscriber's id
	Id uuid.UUID

	// the index of the last read item per partition
	ReadIndex []int

	// read index lock
	subMtx sync.RWMutex
}

func NewSubscriber(partitionCount int) *Subscriber {
	return &Subscriber{
		Id:        uuid.New(),
		ReadIndex: make([]int, partitionCount),
	}
}
