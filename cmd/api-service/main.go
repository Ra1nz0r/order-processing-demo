package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	kafkapub "github.com/Ra1nz0r/order-processing-demo/internal/kafka"
	"github.com/Ra1nz0r/order-processing-demo/internal/logs"
	taskservice "github.com/Ra1nz0r/order-processing-demo/internal/service/task"
	taskv1 "github.com/Ra1nz0r/order-processing-demo/proto/task/v1"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Настраиваем глобальный логгер приложения.
	if err := logs.Setup(logs.LoggerConfig{
		Service:             "api-service",
		IncludeServiceField: true,
		FileName:            "api-service.log",
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
	grpcPort := getEnv("GRPC_PORT", "50051")

	// Создаём Redis-клиент.
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer rdb.Close()

	// Создаём контекст с таймаутом для стартовых проверок.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Проверяем доступность Redis.
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatal().
			Err(err).
			Msg("redis ping error")
	}

	// Создаём Kafka producer для публикации событий.
	producer := kafkapub.NewProducer(kafkaBroker, kafkaTopic)
	defer producer.Close()

	// Открываем TCP-порт для gRPC-сервера.
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("cannot listen server")
	}

	// Создаём экземпляр gRPC-сервера.
	grpcServer := grpc.NewServer()

	// Создаём реализацию TaskService с зависимостями Redis и Kafka
	taskSvc := taskservice.NewService(rdb, producer)

	//  Регистрируем TaskService в gRPC-сервере
	taskv1.RegisterTaskServiceServer(grpcServer, taskSvc)

	// Включаем gRPC reflection для grpcurl и других инструментов
	reflection.Register(grpcServer)

	log.Info().
		Str("redis_addr", redisAddr).
		Msg("redis connected")
	log.Info().
		Str("kafka_broker", kafkaBroker).
		Str("topic", kafkaTopic).
		Msg("kafka producer connected")
	log.Info().
		Str("grpc_port", grpcPort).
		Msg("gRPC server is listening")

	// Запускаем gRPC-сервер и начинаем принимать запросы.
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal().
			Err(err).
			Msg("got serve error")
	}
}

// getEnv возвращает значение переменной окружения или fallback, если она не задана.
func getEnv(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}
