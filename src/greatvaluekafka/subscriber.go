package greatvaluekafka

import "github.com/google/uuid"

type Subscriber struct {
	// the subscriber's id
	Id uuid.UUID

	// the index of the last read item per partition
	ReadIndex []int
}

func NewSubscriber() *Subscriber {
	return &Subscriber{
		Id:        uuid.New(),
		ReadIndex: make([]int, NUM_PARTITIONS),
	}
}
