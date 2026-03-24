package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Ra1nz0r/order-processing-demo/internal/logs"
	"github.com/rs/zerolog/log"
	kgo "github.com/segmentio/kafka-go"
)

// Producer публикует события в Kafka.
type Producer struct {
	writer *kgo.Writer // Writer для отправки сообщений в Kafka.
	topic  string      // Topic, в который публикуются сообщения.
}

// NewProducer создаёт Kafka producer для публикации сообщений в указанный topic.
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

// PublishTaskCreated публикует в Kafka событие о создании задачи.
func (p *Producer) PublishTaskCreated(ctx context.Context, event TaskCreatedEvent) error {
	nFunc, done := logs.LogFunction("kafka.PublishTaskCreated")
	defer done()

	logger := log.With().
		Str("func", nFunc).
		Str("topic", p.topic).
		Str("task_id", event.TaskID).
		Logger()

	// Сериализуем событие в JSON для отправки в Kafka.
	payload, err := json.Marshal(event)
	if err != nil {
		logger.Error().
			Err(err).
			Msg("failed to marshal kafka event")
		return fmt.Errorf("marshal event: %w", err)
	}

	logger.Debug().Msg("publishing task created event")

	// Отправляем сообщение в Kafka.
	err = p.writer.WriteMessages(ctx, kgo.Message{
		Key:   []byte(event.TaskID),
		Value: payload,
	})
	if err != nil {
		logger.Error().
			Err(err).
			Msg("failed to write kafka message")
		return fmt.Errorf("write message: %w", err)
	}

	logger.Debug().Msg("task created event published")

	return nil
}

// Close закрывает Kafka writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}
