package worker

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/JyotinderSingh/task-queue/pkg/common"
	pb "github.com/JyotinderSingh/task-queue/pkg/grpcapi"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	taskProcessTime = 5 * time.Second
	workerPoolSize  = 5 // Number of workers in the pool
)

// WorkerServer represents a gRPC server for handling worker tasks.
// WorkerServer represents a gRPC server for handling worker tasks.
type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	id                       uint32
	serverPort               string
	coordinatorAddress       string
	listener                 net.Listener
	grpcServer               *grpc.Server
	coordinatorServiceClient pb.CoordinatorServiceClient
	heartbeatInterval        time.Duration
	taskQueue                chan *pb.TaskRequest
}

// NewServer creates and returns a new WorkerServer.
func NewServer(port string, coordinator string) *WorkerServer {
	return &WorkerServer{
		id:                 uuid.New().ID(),
		serverPort:         port,
		coordinatorAddress: coordinator,
		heartbeatInterval:  common.DefaultHeartbeat,
		taskQueue:          make(chan *pb.TaskRequest, 100), // Buffered channel
	}
}

// Start initializes and starts the WorkerServer.
func (w *WorkerServer) Start() error {
	w.startWorkerPool(workerPoolSize)

	if err := w.connectToCoordinator(); err != nil {
		return fmt.Errorf("failed to connect to coordinator: %w", err)
	}
	defer w.closeGRPCConnection()

	go w.periodicHeartbeat()

	return w.startGRPCServer()
}

func (w *WorkerServer) connectToCoordinator() error {
	log.Println("Connecting to coordinator...")
	conn, err := grpc.Dial(w.coordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
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
	workerAddress := os.Getenv("WORKER_ADDRESS")
	if workerAddress == "" {
		// Fall back to using the listener address if WORKER_ADDRESS is not set
		workerAddress = w.listener.Addr().String()
	} else {
		workerAddress = workerAddress + w.serverPort
	}

	_, err := w.coordinatorServiceClient.SendHeartbeat(context.Background(), &pb.HeartbeatRequest{
		WorkerId: w.id,
		Address:  workerAddress,
	})
	return err
}

func (w *WorkerServer) startGRPCServer() error {
	var err error

	if w.serverPort == "" {
		// Find a free port using a temporary socket
		w.listener, err = net.Listen("tcp", ":0")                                // Bind to any available port
		w.serverPort = fmt.Sprintf(":%d", w.listener.Addr().(*net.TCPAddr).Port) // Get the assigned port
	} else {
		w.listener, err = net.Listen("tcp", w.serverPort)
	}

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
	w.closeGRPCConnection()

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
	log.Printf("Received task: %+v", req)

	w.taskQueue <- req

	return &pb.TaskResponse{
		Message: "Task was submitted",
		Success: true,
		TaskId:  req.TaskId,
	}, nil
}

// startWorkerPool starts a pool of worker goroutines.
func (w *WorkerServer) startWorkerPool(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		go w.worker()
	}
}

// worker is the function run by each worker goroutine.
func (w *WorkerServer) worker() {
	for task := range w.taskQueue {
		w.coordinatorServiceClient.UpdateTaskStatus(context.Background(),
			&pb.UpdateTaskStatusRequest{
				TaskId: task.GetTaskId(),
				Status: pb.TaskStatus_PROCESSING,
			})
		w.processTask(task)
	}
}

// processTask simulates task processing.
func (w *WorkerServer) processTask(task *pb.TaskRequest) {
	log.Printf("Processing task: %+v", task)
	time.Sleep(taskProcessTime)
	log.Printf("Completed task: %+v", task)
	w.coordinatorServiceClient.UpdateTaskStatus(context.Background(),
		&pb.UpdateTaskStatusRequest{
			TaskId: task.GetTaskId(),
			Status: pb.TaskStatus_COMPLETE,
		})
}
