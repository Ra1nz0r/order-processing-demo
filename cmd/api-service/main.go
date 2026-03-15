package main

import (
	"context"
	"log"
	"net"
	"time"

	kafkapub "github.com/Ra1nz0r/order-processing-demo/internal/kafka"
	taskservice "github.com/Ra1nz0r/order-processing-demo/internal/service/task"
	taskv1 "github.com/Ra1nz0r/order-processing-demo/proto/task/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	redisAddr   = "localhost:6379"
	kafkaBroker = "localhost:9092"
	kafkaTopic  = "tasks.created"
)

func main() {
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

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen error: %v", err)
	}

	grpcServer := grpc.NewServer()

	taskSvc := taskservice.NewService(rdb, producer)
	taskv1.RegisterTaskServiceServer(grpcServer, taskSvc)

	reflection.Register(grpcServer)

	log.Println("redis connected")
	log.Println("kafka producer connected")
	log.Println("gRPC server is listening on :50051")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve error: %v", err)
	}
}
