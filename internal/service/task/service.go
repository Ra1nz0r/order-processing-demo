package task

import (
	"context"
	"sync"

	taskv1 "github.com/Ra1nz0r/order-processing-demo/proto/task/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	taskv1.UnimplementedTaskServiceServer

	mu     sync.RWMutex
	status map[string]string
}

func NewService() *Service {
	return &Service{
		status: make(map[string]string),
	}
}

func (s *Service) CreateTask(ctx context.Context, req *taskv1.CreateTaskRequest) (*taskv1.CreateTaskResponse, error) {
	if req.GetTitle() == "" {
		return nil, status.Error(codes.InvalidArgument, "title is required")
	}

	taskID := uuid.NewString()

	s.mu.Lock()
	s.status[taskID] = "created"
	s.mu.Unlock()

	return &taskv1.CreateTaskResponse{
		TaskId: taskID,
		Status: "created",
	}, nil
}

func (s *Service) GetTaskStatus(ctx context.Context, req *taskv1.GetTaskStatusRequest) (*taskv1.GetTaskStatusResponse, error) {
	if req.GetTaskId() == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id is required")
	}

	s.mu.RLock()
	taskStatus, ok := s.status[req.GetTaskId()]
	s.mu.RUnlock()

	if !ok {
		return nil, status.Error(codes.NotFound, "task not found")
	}

	return &taskv1.GetTaskStatusResponse{
		TaskId: req.GetTaskId(),
		Status: taskStatus,
	}, nil
}
