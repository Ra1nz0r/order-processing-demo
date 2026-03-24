package task

import (
	"context"
	"fmt"

	kafkapub "github.com/Ra1nz0r/order-processing-demo/internal/kafka"
	"github.com/Ra1nz0r/order-processing-demo/internal/logs"
	taskv1 "github.com/Ra1nz0r/order-processing-demo/proto/task/v1"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service реализует gRPC-сервис TaskService.
type Service struct {
	taskv1.UnimplementedTaskServiceServer

	rdb      *redis.Client      // Redis-клиент для хранения статусов задач.
	producer *kafkapub.Producer // Kafka producer для публикации событий.
}

// NewService создаёт экземпляр TaskService с зависимостями Redis и Kafka.
func NewService(rdb *redis.Client, producer *kafkapub.Producer) *Service {
	return &Service{
		rdb:      rdb,
		producer: producer,
	}
}

// CreateTask создаёт новую задачу, сохраняет начальный статус в Redis
// и публикует событие о создании задачи в Kafka.
func (s *Service) CreateTask(ctx context.Context, req *taskv1.CreateTaskRequest) (*taskv1.CreateTaskResponse, error) {
	nFunc, done := logs.LogFunction("task.CreateTask")
	defer done()

	logger := log.With().
		Str("func", nFunc).
		Str("title", req.GetTitle()).
		Logger()

	if req.GetTitle() == "" {
		logger.Warn().Msg("validation failed: title is required")
		return nil, status.Error(codes.InvalidArgument, "title is required")
	}

	taskID := uuid.NewString()
	key := taskStatusKey(taskID)

	logger = logger.With().
		Str("task_id", taskID).
		Str("redis_key", key).
		Logger()

	if err := s.rdb.Set(ctx, key, "created", 0).Err(); err != nil {
		logger.Error().
			Err(err).
			Msg("failed to save task status in redis")
		return nil, status.Error(codes.Internal, "failed to save task status")
	}

	event := kafkapub.TaskCreatedEvent{
		TaskID:  taskID,
		Title:   req.GetTitle(),
		Payload: req.GetPayload(),
	}

	if err := s.producer.PublishTaskCreated(ctx, event); err != nil {
		logger.Error().
			Err(err).
			Msg("failed to publish task event")
		return nil, status.Error(codes.Internal, "failed to publish task event")
	}

	logger.Info().
		Msg("task created successfully")

	return &taskv1.CreateTaskResponse{
		TaskId: taskID,
		Status: "created",
	}, nil
}

// GetTaskStatus возвращает текущий статус задачи из Redis по task_id.
func (s *Service) GetTaskStatus(ctx context.Context, req *taskv1.GetTaskStatusRequest) (*taskv1.GetTaskStatusResponse, error) {
	nFunc, done := logs.LogFunction("task.GetTaskStatus")
	defer done()

	logger := log.With().
		Str("func", nFunc).
		Str("task_id", req.GetTaskId()).
		Logger()

	if req.GetTaskId() == "" {
		logger.Warn().Msg("validation failed: task_id is required")
		return nil, status.Error(codes.InvalidArgument, "task_id is required")
	}

	key := taskStatusKey(req.GetTaskId())

	taskStatus, err := s.rdb.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			logger.Warn().
				Str("redis_key", key).
				Msg("task not found")
			return nil, status.Error(codes.NotFound, "task not found")
		}

		logger.Error().
			Err(err).
			Str("redis_key", key).
			Msg("failed to get task status from redis")
		return nil, status.Error(codes.Internal, "failed to get task status")
	}

	logger.Debug().
		Str("redis_key", key).
		Str("status", taskStatus).
		Msg("task status fetched successfully")

	return &taskv1.GetTaskStatusResponse{
		TaskId: req.GetTaskId(),
		Status: taskStatus,
	}, nil
}

// taskStatusKey формирует Redis-ключ для хранения статуса задачи.
func taskStatusKey(taskID string) string {
	return fmt.Sprintf("task:%s:status", taskID)
}
