package worker

import (
	"context"
	"log"
	"net"
	"time"

	pb "github.com/JyotinderSingh/task-queue/pkg/grpcapi"
	"google.golang.org/grpc"
)

const serverPort = ":50051"

// WorkerServer represents a gRPC server for handling worker tasks.
type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	listener   net.Listener
	grpcServer *grpc.Server
}

// NewServer creates and returns a new WorkerServer.
func NewServer() *WorkerServer {
	return &WorkerServer{}
}

// Start initializes and starts the WorkerServer.
func (w *WorkerServer) Start() error {
	var err error
	w.listener, err = net.Listen("tcp", serverPort)
	if err != nil {
		log.Printf("Failed to listen: %v", err)
		return err
	}

	log.Println("Starting worker server on", serverPort)
	w.grpcServer = grpc.NewServer()
	pb.RegisterWorkerServiceServer(w.grpcServer, &WorkerServer{})

	if err := w.grpcServer.Serve(w.listener); err != nil {
		log.Printf("Failed to serve: %v", err)
		return err
	}

	return nil
}

// Stop gracefully shuts down the WorkerServer.
func (w *WorkerServer) Stop() error {
	if w.grpcServer != nil {
		w.grpcServer.GracefulStop()
	}

	if w.listener != nil {
		if err := w.listener.Close(); err != nil {
			log.Printf("Failed to close the listener: %v", err)
			return err
		}
	}

	log.Println("Worker server stopped")
	return nil
}

// SubmitTask handles the submission of a task to the worker server.
func (s *WorkerServer) SubmitTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("Received task: %s", req.GetData())
	go processTask(req.GetData())

	return &pb.TaskResponse{
		Message: "Task was submitted",
		Success: true,
		TaskId:  req.TaskId,
	}, nil
}

// processTask simulates task processing.
func processTask(data string) {
	log.Printf("Starting task: %s", data)
	time.Sleep(5 * time.Second) // Simulating task processing time
	log.Printf("Completed task: %s", data)
}
