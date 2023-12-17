package worker

import (
	"context"
	"log"
	"net"
	"time"

	pb "github.com/JyotinderSingh/task-queue/pkg/grpcapi"
	"google.golang.org/grpc"
)

type WorkerServer struct {
	listener     net.Listener
	workerServer *grpc.Server
}

type workerServiceServer struct {
	pb.UnimplementedWorkerServiceServer
}

func (s *workerServiceServer) SubmitTask(ctx context.Context, in *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("Received: %v", in.GetData())
	// Process the task asynchronously
	go processTask(in.GetData())
	return &pb.TaskResponse{Message: "Task was submitted", Success: true, TaskId: in.TaskId}, nil
}

func processTask(data string) {
	// Process the task data
	log.Println("Starting Task", data)
	time.Sleep(5 * time.Second)
	log.Println("Completed Task", data)
}

func (w *WorkerServer) Start() error {
	PORT := ":50051"
	lis, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Printf("failed to listen: %v", err)
		return err // Return the error instead of logging fatal
	}
	log.Println("Started worker server at", PORT)
	w.listener = lis
	w.workerServer = grpc.NewServer()
	pb.RegisterWorkerServiceServer(w.workerServer, &workerServiceServer{})
	if err := w.workerServer.Serve(lis); err != nil {
		log.Printf("failed to serve: %v", err)
		return err // Return the error instead of logging fatal
	}

	return nil // Return nil if no errors occurred
}

// Stop gracefully shuts down the server
func (w *WorkerServer) Stop() error {
	// Gracefully stop the server
	w.workerServer.GracefulStop()

	// Close the listener
	if err := w.listener.Close(); err != nil {
		log.Printf("failed to close the listener: %v", err)
		return err
	}

	log.Println("Worker server stopped")
	return nil
}

func NewServer() *WorkerServer {
	return &WorkerServer{}
}
