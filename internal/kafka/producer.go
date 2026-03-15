package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	kgo "github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kgo.Writer
	topic  string
}

func NewProducer(brokerAddr, topic string) *Producer {
	writer := &kgo.Writer{
		Addr:                   kgo.TCP(brokerAddr),
		Topic:                  topic,
		Balancer:               &kgo.LeastBytes{},
		AllowAutoTopicCreation: true,
	}

	return &Producer{
		writer: writer,
		topic:  topic,
	}
}

func (p *Producer) PublishTaskCreated(ctx context.Context, event TaskCreatedEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	err = p.writer.WriteMessages(ctx, kgo.Message{
		Key:   []byte(event.TaskID),
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("write message: %w", err)
	}

	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
