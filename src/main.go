package main

import (
	"lab4/src/greatvaluekafka"
	"log"
	"net/rpc"
)

// this is a test driver just to see if the controller works
func main() {
	bOpts := &greatvaluekafka.BrokerOpts{
		BrokerIndex: 0,
		BrokerAddr:  "localhost:9696",
		ControlAddr: "localhost:6969",
	}

	go greatvaluekafka.NewBroker(bOpts)

	client, err := rpc.DialHTTP("tcp", bOpts.ControlAddr)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	client.Call("BrokerRPC.Activate", struct{}{}, nil)

	select {}
}
