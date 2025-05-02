package greatvaluekafka

import (
	"strconv"
	"sync"
	"testing"
)

const (
	defaultMaxSize = 10
	largeMaxSize   = 512
)

// createPartition creates a new partition with the given max size
func createPartition(maxSize int) *Partition {
	return NewPartition(&partitionOpts{
		maxSize: maxSize,
	})
}

// createSubscribers creates a slice of subscriber
func createSubscribers(subCount int) []*Subscriber {
	subs := make([]*Subscriber, subCount)
	for i := 0; i < subCount; i++ {
		subs[i] = NewSubscriber(1)
	}
	return subs
}

func Test_Partition_EnqueueMessage(t *testing.T) {
	// create a new partition
	p := createPartition(defaultMaxSize)

	// check if the partition is empty
	if len(p.queue) != 0 {
		t.Fatalf("Expected partition to be empty, got %d", len(p.queue))
	}

	// put 5 bytes into the partition
	pi := NewPartitionItem([]byte("aaaaa"))
	p.Enqueue(pi)

	// check if the partition has one message
	if len(p.queue) != 1 {
		t.Fatalf("Expected partition to have one message, got %d", len(p.queue))
	}

	// check if the message is correct
	if string(p.queue[0].Message) != string(pi.Message) {
		t.Fatalf("Expected message to be %v, got %v", string(pi.Message), string(p.queue[0].Message))
	}
}

func Test_Partition_EnqueueTooMany(t *testing.T) {
	// create a new partition
	p := createPartition(defaultMaxSize)

	// check if the partition is empty
	if len(p.queue) != 0 {
		t.Fatalf("Expected partition to be empty, got %d", len(p.queue))
	}

	// put 10 a's into the partition
	pi := NewPartitionItem([]byte("aaaaaaaaaa"))
	p.Enqueue(pi)

	// check if the partition has one message
	if len(p.queue) != 1 {
		t.Fatalf("Expected partition to have one message, got %d", len(p.queue))
	}

	// enqueue 5 b's, which should evict the 10-byte item
	pi2 := NewPartitionItem([]byte("bbbbb"))
	p.Enqueue(pi2)

	// still only one message, and it should be the b’s
	if len(p.queue) != 1 {
		t.Fatalf("Expected partition to have one message, got %d", len(p.queue))
	}
	if string(p.queue[0].Message) != string(pi2.Message) {
		t.Fatalf("Expected message to be %v, got %v",
			string(pi2.Message), string(p.queue[0].Message))
	}

	// now enqueue ten 10-byte messages (9 a's + digit); each should evict the previous
	for i := 0; i < 10; i++ {
		pi = NewPartitionItem([]byte("aaaaaaaaa" + strconv.Itoa(i)))
		p.Enqueue(pi)
	}

	// still only one message in the queue
	if len(p.queue) != 1 {
		t.Fatalf("Expected partition to have one message, got %d", len(p.queue))
	}

	// and it should be the last one: nine "a"s and "9"
	want := "aaaaaaaaa9"
	got := string(p.queue[0].Message)
	if got != want {
		t.Fatalf("Expected message to be %q, got %q", want, got)
	}
}

func Test_Partition_Enqueue_Concurrent(t *testing.T) {
	// create a new partition
	p := createPartition(largeMaxSize)

	var wg sync.WaitGroup
	for i := 0; i < largeMaxSize; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pi := NewPartitionItem([]byte{byte(i)})
			p.Enqueue(pi)
		}(i)
	}
	wg.Wait()

	if len(p.queue) != largeMaxSize {
		t.Fatalf("Expected partition to have %d messages, got %d", largeMaxSize, len(p.queue))
	}
}

func Test_Partition_Enqueue_TooManyConcurrent(t *testing.T) {
	// create a new partition
	p := createPartition(defaultMaxSize)

	// put pressure on a small partition
	var wg sync.WaitGroup
	for i := 0; i < largeMaxSize; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pi := NewPartitionItem([]byte{byte(i)})
			p.Enqueue(pi)
		}(i)
	}
	wg.Wait()

	if len(p.queue) != defaultMaxSize {
		t.Fatalf("Expected partition to have %d messages, got %d", defaultMaxSize, len(p.queue))
	}
}
