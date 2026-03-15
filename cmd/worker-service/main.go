package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os/signal"
	"syscall"
	"time"

	kafkapub "github.com/Ra1nz0r/order-processing-demo/internal/kafka"

	"github.com/redis/go-redis/v9"
	kgo "github.com/segmentio/kafka-go"
)

const (
	redisAddr   = "localhost:6379"
	kafkaBroker = "localhost:9092"
	kafkaTopic  = "tasks.created"
	consumerGrp = "task-worker-group"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer rdb.Close()

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Fatalf("redis ping error: %v", err)
	}

	reader := kgo.NewReader(kgo.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		GroupID:  consumerGrp,
		Topic:    kafkaTopic,
		MaxBytes: 10e6,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("reader close error: %v", err)
		}
	}()

	log.Println("worker-service started")
	log.Printf("kafka broker: %s, topic: %s, group: %s", kafkaBroker, kafkaTopic, consumerGrp)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Println("worker stopped")
				return
			}

			log.Printf("read message error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		var event kafkapub.TaskCreatedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("unmarshal event error: %v", err)
			continue
		}

		log.Printf("received task: id=%s title=%q payload=%q", event.TaskID, event.Title, event.Payload)

		if err := setTaskStatus(ctx, rdb, event.TaskID, "processing"); err != nil {
			log.Printf("set processing status error: %v", err)
			continue
		}

		time.Sleep(3 * time.Second)

		if err := setTaskStatus(ctx, rdb, event.TaskID, "done"); err != nil {
			log.Printf("set done status error: %v", err)
			continue
		}

		log.Printf("task %s processed successfully", event.TaskID)
	}
}

func setTaskStatus(ctx context.Context, rdb *redis.Client, taskID, status string) error {
	key := "task:" + taskID + ":status"
	return rdb.Set(ctx, key, status, 0).Err()
}
