package worker

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/JyotinderSingh/task-queue/pkg/grpcapi"
	"google.golang.org/grpc"
)

type workerServiceServer struct {
	pb.UnimplementedWorkerServiceServer
}

func (s *workerServiceServer) SendTask(ctx context.Context, in *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("Received: %v", in.GetData())
	// Process the task asynchronously
	go processTask(in.GetData())
	return &pb.TaskResponse{Result: "Task received and is being processed"}, nil
}

func processTask(data string) {
	// Process the task data
	fmt.Println("Starting Task", data)
	time.Sleep(5 * time.Second)
	fmt.Println("Completed Task", data)
}

func Start() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterWorkerServiceServer(s, &workerServiceServer{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
