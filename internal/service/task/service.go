package task

import (
	"context"
	"fmt"

	kafkapub "github.com/Ra1nz0r/order-processing-demo/internal/kafka"
	taskv1 "github.com/Ra1nz0r/order-processing-demo/proto/task/v1"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	taskv1.UnimplementedTaskServiceServer

	rdb      *redis.Client
	producer *kafkapub.Producer
}

func NewService(rdb *redis.Client, producer *kafkapub.Producer) *Service {
	return &Service{
		rdb:      rdb,
		producer: producer,
	}
}

func (s *Service) CreateTask(ctx context.Context, req *taskv1.CreateTaskRequest) (*taskv1.CreateTaskResponse, error) {
	if req.GetTitle() == "" {
		return nil, status.Error(codes.InvalidArgument, "title is required")
	}

	taskID := uuid.NewString()

	key := taskStatusKey(taskID)
	if err := s.rdb.Set(ctx, key, "created", 0).Err(); err != nil {
		return nil, status.Error(codes.Internal, "failed to save task status")
	}

	event := kafkapub.TaskCreatedEvent{
		TaskID:  taskID,
		Title:   req.GetTitle(),
		Payload: req.GetPayload(),
	}

	if err := s.producer.PublishTaskCreated(ctx, event); err != nil {
		return nil, status.Error(codes.Internal, "failed to publish task event")
	}

	return &taskv1.CreateTaskResponse{
		TaskId: taskID,
		Status: "created",
	}, nil
}

func (s *Service) GetTaskStatus(ctx context.Context, req *taskv1.GetTaskStatusRequest) (*taskv1.GetTaskStatusResponse, error) {
	if req.GetTaskId() == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id is required")
	}

	key := taskStatusKey(req.GetTaskId())

	taskStatus, err := s.rdb.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, status.Error(codes.NotFound, "task not found")
		}
		return nil, status.Error(codes.Internal, "failed to get task status")
	}

	return &taskv1.GetTaskStatusResponse{
		TaskId: req.GetTaskId(),
		Status: taskStatus,
	}, nil
}

func taskStatusKey(taskID string) string {
	return fmt.Sprintf("task:%s:status", taskID)
}
