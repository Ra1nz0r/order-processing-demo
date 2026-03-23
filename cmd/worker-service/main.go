package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	kafkapub "github.com/Ra1nz0r/order-processing-demo/internal/kafka"
	"github.com/Ra1nz0r/order-processing-demo/internal/logs"
	"github.com/rs/zerolog/log"

	"github.com/redis/go-redis/v9"
	kgo "github.com/segmentio/kafka-go"
)

func main() {
	// Настраиваем глобальный логгер приложения.
	if err := logs.Setup(logs.LoggerConfig{
		Service:             "worker-service",
		IncludeServiceField: true,
		FileName:            "worker-service.log",
		Level:               getEnv("LOG_LEVEL", "trace"),
		Pretty:              true,
		ToFile:              true,
		Dir:                 getEnv("LOG_DIR", "log"),
		Caller:              true,
	}); err != nil {
		panic(fmt.Errorf("failed setup logger: %w", err))
	}

	// Читаем адреса и параметры запуска из переменных окружения.
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	kafkaBroker := getEnv("KAFKA_BROKER", "localhost:9092")
	kafkaTopic := getEnv("KAFKA_TOPIC", "tasks.created")
	consumerGrp := getEnv("KAFKA_GROUP_ID", "task-worker-local")

	// Создаём контекст, который завершится по SIGINT или SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Создаём Redis-клиент.
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer rdb.Close()

	// Создаём контекст с таймаутом для стартовых проверок.
	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Проверяем доступность Redis.
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Fatal().
			Err(err).
			Str("redis_addr", redisAddr).
			Msg("redis ping error")
	}

	// Создаём Kafka reader для чтения событий из topic.
	reader := kgo.NewReader(kgo.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		GroupID:  consumerGrp,
		Topic:    kafkaTopic,
		MaxBytes: 10e6,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Error().
				Err(err).
				Msg("failed to close kafka reader")
		}
	}()

	log.Info().
		Str("redis_addr", redisAddr).
		Str("kafka_broker", kafkaBroker).
		Str("topic", kafkaTopic).
		Str("group_id", consumerGrp).
		Msg("worker-service started")

	for {
		// Читаем следующее сообщение из Kafka.
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Info().Msg("worker-service stopped")
				return
			}

			log.Error().
				Err(err).
				Str("kafka_broker", kafkaBroker).
				Str("topic", kafkaTopic).
				Str("group_id", consumerGrp).
				Msg("failed to read kafka message")

			time.Sleep(1 * time.Second)
			continue
		}

		var event kafkapub.TaskCreatedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Error().
				Err(err).
				Str("topic", msg.Topic).
				Int("partition", msg.Partition).
				Int64("offset", msg.Offset).
				Bytes("message", msg.Value).
				Msg("failed to unmarshal kafka message")
			continue
		}

		log.Info().
			Str("task_id", event.TaskID).
			Str("title", event.Title).
			Str("payload", event.Payload).
			Int("partition", msg.Partition).
			Int64("offset", msg.Offset).
			Msg("task received from kafka")

		if err := setTaskStatus(ctx, rdb, event.TaskID, "processing"); err != nil {
			log.Error().
				Err(err).
				Str("task_id", event.TaskID).
				Str("status", "processing").
				Msg("failed to set task status")
			continue
		}

		log.Debug().
			Str("task_id", event.TaskID).
			Str("status", "processing").
			Msg("task status updated")

		time.Sleep(3 * time.Second)

		if err := setTaskStatus(ctx, rdb, event.TaskID, "done"); err != nil {
			log.Error().
				Err(err).
				Str("task_id", event.TaskID).
				Str("status", "done").
				Msg("failed to set task status")
			continue
		}

		log.Info().
			Str("task_id", event.TaskID).
			Str("status", "done").
			Msg("task processed successfully")
	}
}

// setTaskStatus обновляет статус задачи в Redis.
func setTaskStatus(ctx context.Context, rdb *redis.Client, taskID, status string) error {
	key := "task:" + taskID + ":status"
	return rdb.Set(ctx, key, status, 0).Err()
}

// getEnv возвращает значение переменной окружения или fallback, если она не задана.
func getEnv(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}
