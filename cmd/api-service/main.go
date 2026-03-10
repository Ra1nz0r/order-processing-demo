package main

import (
	"log"
	"net"

	taskservice "github.com/Ra1nz0r/order-processing-demo/internal/service/task"
	taskv1 "github.com/Ra1nz0r/order-processing-demo/proto/task/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen error: %v", err)
	}

	grpcServer := grpc.NewServer()

	taskSvc := taskservice.NewService()
	taskv1.RegisterTaskServiceServer(grpcServer, taskSvc)

	reflection.Register(grpcServer)

	log.Println("gRPC server is listening on :50051")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve error: %v", err)
	}
}
