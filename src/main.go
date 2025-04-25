package main

import "lab4/src/greatvaluekafka"

func main() {
	bOpts := &greatvaluekafka.BrokerOpts{
		BrokerIndex: 0,
		BrokerAddr:  "localhost:6969",
	}

	broker := greatvaluekafka.NewBroker(bOpts)
	broker.Activate()

	select {}
}
