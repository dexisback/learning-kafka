package producer

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"github.com/dexisback/learning-kafka/internal/event"


)


type Producer struct {
	writer *kafka.Writer
}

func New(broker, topic string )*Producer{
	return &Producer{
		writer: &kafka.Writer{
			Addr: kafka.TCP(broker),
			Topic: topic, 
			Balancer: &kafka.Hash{},
		},
	}
}

func(p *Producer) Publish(ctx context.Context, order event.OrderCreated) error {
	data, err := json.Marshal(order)
	if err != nil{
		return err 
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key: []byte(order.UserID),
		Value: data, 
	})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}