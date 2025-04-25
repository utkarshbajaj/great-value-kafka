package greatvaluekafka

type BrokerRPC struct {
	Broker *Broker
}

func (b *BrokerRPC) Activate(_ struct{}, _ *struct{}) error {
	return b.Broker.Activate()
}

func (b *BrokerRPC) Deactivate(_ struct{}, _ *struct{}) error {
	return b.Broker.Deactivate()
}

func (b *BrokerRPC) Terminate(_ struct{}, _ *struct{}) error {
	return b.Broker.Terminate()
}

func (b *BrokerRPC) GetStatus(_ struct{}, reply *BrokerStatusReport) error {
	status, err := b.Broker.GetStatus()
	*reply = status
	return err
}
