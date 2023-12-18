package worker

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/JyotinderSingh/task-queue/pkg/grpcapi"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	coordinatorAddr  = "localhost:50050"
	defaultHeartbeat = 5 * time.Second
	taskProcessTime  = 5 * time.Second
)

// WorkerServer represents a gRPC server for handling worker tasks.
// WorkerServer represents a gRPC server for handling worker tasks.
type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	id                       uint32
	serverPort               string
	listener                 net.Listener
	grpcServer               *grpc.Server
	coordinatorServiceClient pb.CoordinatorServiceClient
	heartbeatInterval        time.Duration
}

// NewServer creates and returns a new WorkerServer.
func NewServer(port string) *WorkerServer {
	return &WorkerServer{
		id:                uuid.New().ID(),
		serverPort:        port,
		heartbeatInterval: defaultHeartbeat,
	}
}

// Start initializes and starts the WorkerServer.
func (w *WorkerServer) Start() error {
	if err := w.connectToCoordinator(); err != nil {
		return fmt.Errorf("failed to connect to coordinator: %w", err)
	}
	defer w.closeGRPCConnection()

	go w.periodicHeartbeat()

	return w.startGRPCServer()
}

func (w *WorkerServer) connectToCoordinator() error {
	log.Println("Connecting to coordinator...")
	conn, err := grpc.Dial(coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return err
	}

	w.coordinatorServiceClient = pb.NewCoordinatorServiceClient(conn)
	log.Println("Connected to coordinator!")
	return nil
}

func (w *WorkerServer) periodicHeartbeat() {
	ticker := time.NewTicker(w.heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		if err := w.sendHeartbeat(); err != nil {
			log.Printf("Failed to send heartbeat: %v", err)
			break
		}
	}
}

func (w *WorkerServer) sendHeartbeat() error {
	_, err := w.coordinatorServiceClient.SendHeartbeat(context.Background(), &pb.HeartbeatRequest{
		WorkerId: w.id,
		Address:  w.listener.Addr().String(),
	})
	return err
}

func (w *WorkerServer) startGRPCServer() error {
	var err error
	w.listener, err = net.Listen("tcp", w.serverPort)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", w.serverPort, err)
	}

	log.Printf("Starting worker server on %s\n", w.serverPort)
	w.grpcServer = grpc.NewServer()
	pb.RegisterWorkerServiceServer(w.grpcServer, w)

	return w.grpcServer.Serve(w.listener)
}

// Stop gracefully shuts down the WorkerServer.
func (w *WorkerServer) Stop() error {
	if w.grpcServer != nil {
		w.grpcServer.GracefulStop()
	}

	if w.listener != nil {
		if err := w.listener.Close(); err != nil {
			return fmt.Errorf("failed to close the listener: %w", err)
		}
	}

	log.Println("Worker server stopped")
	return nil
}

func (w *WorkerServer) closeGRPCConnection() {
	if w.grpcServer != nil {
		w.grpcServer.GracefulStop()
	}
}

// SubmitTask handles the submission of a task to the worker server.
func (w *WorkerServer) SubmitTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
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
	log.Printf("Processing task: %s", data)
	time.Sleep(taskProcessTime)
	log.Printf("Completed task: %s", data)
}
