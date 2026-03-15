package main

import (
	"context"
	"log"
	"net"
	"os"
	"time"

	kafkapub "github.com/Ra1nz0r/order-processing-demo/internal/kafka"
	taskservice "github.com/Ra1nz0r/order-processing-demo/internal/service/task"
	taskv1 "github.com/Ra1nz0r/order-processing-demo/proto/task/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	kafkaBroker := getEnv("KAFKA_BROKER", "localhost:9092")
	kafkaTopic := getEnv("KAFKA_TOPIC", "tasks.created")
	grpcPort := getEnv("GRPC_PORT", "50051")

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping error: %v", err)
	}

	producer := kafkapub.NewProducer(kafkaBroker, kafkaTopic)
	defer producer.Close()

	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("listen error: %v", err)
	}

	grpcServer := grpc.NewServer()

	taskSvc := taskservice.NewService(rdb, producer)
	taskv1.RegisterTaskServiceServer(grpcServer, taskSvc)
	reflection.Register(grpcServer)

	log.Printf("redis connected: %s", redisAddr)
	log.Printf("kafka producer connected: %s", kafkaBroker)
	log.Printf("gRPC server is listening on :%s", grpcPort)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve error: %v", err)
	}
}

func getEnv(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}
