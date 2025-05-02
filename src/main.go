package main

import (
	"lab4/src/greatvaluekafka"
	"log"
	"net/rpc"
)

// this is a test driver just to see if the controller works
func main() {
	bOpts := &greatvaluekafka.BrokerOpts{
		BrokerIndex:   0,
		BrokerAddr:    "localhost:9696",
		ControlAddr:   "localhost:6969",
		DebugPath:     "/debug",
		RPCPath:       "/rpc",
		NumPartitions: 2,
	}

	go greatvaluekafka.NewBroker(bOpts)

	client, err := rpc.DialHTTPPath("tcp", bOpts.ControlAddr, "/rpc")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	client.Call("BrokerRPC.Activate", struct{}{}, nil)

	select {}
}
