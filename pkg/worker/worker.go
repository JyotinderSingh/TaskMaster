package worker

import (
	"context"
	"log"
	"net"
	"time"

	pb "github.com/JyotinderSingh/task-queue/pkg/grpcapi"
	"google.golang.org/grpc"
)

type workerServer struct {
	listener     net.Listener
	workerServer *grpc.Server
}

type workerServiceServer struct {
	pb.UnimplementedWorkerServiceServer
}

func (s *workerServiceServer) SendTask(ctx context.Context, in *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("Received: %v", in.GetData())
	// Process the task asynchronously
	processTask(in.GetData())
	return &pb.TaskResponse{Result: "Task processed"}, nil
}

func processTask(data string) {
	// Process the task data
	log.Println("Starting Task", data)
	time.Sleep(5 * time.Second)
	log.Println("Completed Task", data)
}

func (w *workerServer) Start() {
	PORT := ":50051"
	lis, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Println("Started worker server at", PORT)
	w.listener = lis
	w.workerServer = grpc.NewServer()
	pb.RegisterWorkerServiceServer(w.workerServer, &workerServiceServer{})
	if err := w.workerServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func NewServer() workerServer {
	return workerServer{}
}
