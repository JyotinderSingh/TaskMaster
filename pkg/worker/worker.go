package worker

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
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
type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	id                       uint32
	serverPort               string
	coordinatorAddress       string
	listener                 net.Listener
	grpcServer               *grpc.Server
	coordinatorConnection    *grpc.ClientConn
	coordinatorServiceClient pb.CoordinatorServiceClient
	heartbeatInterval        time.Duration
	taskQueue                chan *pb.TaskRequest
	ReceivedTasks            map[string]*pb.TaskRequest
	ReceivedTasksMutex       sync.Mutex
	ctx                      context.Context    // The root context for all goroutines
	cancel                   context.CancelFunc // Function to cancel the context
	wg                       sync.WaitGroup     // WaitGroup to wait for all goroutines to finish
}

// NewServer creates and returns a new WorkerServer.
func NewServer(port string, coordinator string) *WorkerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerServer{
		id:                 uuid.New().ID(),
		serverPort:         port,
		coordinatorAddress: coordinator,
		heartbeatInterval:  common.DefaultHeartbeat,
		taskQueue:          make(chan *pb.TaskRequest, 100), // Buffered channel
		ReceivedTasks:      make(map[string]*pb.TaskRequest),
		ctx:                ctx,
		cancel:             cancel,
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

	if err := w.startGRPCServer(); err != nil {
		return fmt.Errorf("gRPC server start failed: %w", err)
	}

	return w.awaitShutdown()
}

func (w *WorkerServer) connectToCoordinator() error {
	log.Println("Connecting to coordinator...")
	var err error
	w.coordinatorConnection, err = grpc.Dial(w.coordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return err
	}

	w.coordinatorServiceClient = pb.NewCoordinatorServiceClient(w.coordinatorConnection)
	log.Println("Connected to coordinator!")
	return nil
}

func (w *WorkerServer) periodicHeartbeat() {
	w.wg.Add(1)       // Add this goroutine to the waitgroup.
	defer w.wg.Done() // Signal this goroutine is done when the function returns

	ticker := time.NewTicker(w.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := w.sendHeartbeat(); err != nil {
				log.Printf("Failed to send heartbeat: %v", err)
				return
			}
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *WorkerServer) sendHeartbeat() error {
	workerAddress := os.Getenv("WORKER_ADDRESS")
	if workerAddress == "" {
		// Fall back to using the listener address if WORKER_ADDRESS is not set
		workerAddress = w.listener.Addr().String()
	} else {
		workerAddress += w.serverPort
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

	go func() {
		if err := w.grpcServer.Serve(w.listener); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	return nil
}

func (w *WorkerServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	return w.Stop()
}

// Stop gracefully shuts down the WorkerServer.
func (w *WorkerServer) Stop() error {
	// Signal all goroutines to stop
	w.cancel()
	// Wait for all goroutines to finish
	w.wg.Wait()

	w.closeGRPCConnection()
	log.Println("Worker server stopped")
	return nil
}

func (w *WorkerServer) closeGRPCConnection() {
	if w.grpcServer != nil {
		w.grpcServer.GracefulStop()
	}

	if w.listener != nil {
		if err := w.listener.Close(); err != nil {
			log.Printf("Error while closing the listener: %v", err)
		}
	}

	if err := w.coordinatorConnection.Close(); err != nil {
		log.Printf("Error while closing client connection with coordinator: %v", err)
	}
}

// SubmitTask handles the submission of a task to the worker server.
func (w *WorkerServer) SubmitTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("Received task: %+v", req)

	w.ReceivedTasksMutex.Lock()
	w.ReceivedTasks[req.GetTaskId()] = req
	w.ReceivedTasksMutex.Unlock()

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
		w.wg.Add(1)
		go w.worker()
	}
}

// worker is the function run by each worker goroutine.
func (w *WorkerServer) worker() {
	defer w.wg.Done() // Signal this worker is done when the function returns.

	for {
		select {
		case task := <-w.taskQueue:
			go w.updateTaskStatus(task, pb.TaskStatus_STARTED)
			w.processTask(task)
			go w.updateTaskStatus(task, pb.TaskStatus_COMPLETE)
		case <-w.ctx.Done():
			return
		}
	}
}

// updateTaskStatus updates the status of a task.
func (w *WorkerServer) updateTaskStatus(task *pb.TaskRequest, status pb.TaskStatus) {
	w.coordinatorServiceClient.UpdateTaskStatus(context.Background(), &pb.UpdateTaskStatusRequest{
		TaskId:      task.GetTaskId(),
		Status:      status,
		StartedAt:   time.Now().Unix(),
		CompletedAt: time.Now().Unix(),
	})
}

// processTask simulates task processing.
func (w *WorkerServer) processTask(task *pb.TaskRequest) {
	log.Printf("Processing task: %+v", task)
	time.Sleep(taskProcessTime)
	log.Printf("Completed task: %+v", task)
}
