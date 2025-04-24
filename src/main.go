package main

import "fmt"
import "greatvaluekafka/src/greatvaluekafka"

func main() {
	// create a dummy subscriber
	subscriber := &greatvaluekafka.Subscriber{
		ReadIndex: 0,
	}

	// create a partition
	partition := greatvaluekafka.NewPartition()

	// create a partition item

	for i := 0; i < 5; i++ {
		// create a partition item

		// create a big partition item using a for loop
		var bigPartitionItem []byte
		for j := 0; j < 512; j++ {
			bigPartitionItem = append(bigPartitionItem, byte(i))
		}

		item := greatvaluekafka.NewPartitionItem(bigPartitionItem)

		// enqueue the item
		partition.Enqueue(item)
	}

	pi := partition.ReadBySub(subscriber)

	fmt.Printf("Item: %v\n", pi.Message)

	pi = partition.ReadBySub(subscriber)
	fmt.Printf("Item: %v\n", pi.Message)
}
