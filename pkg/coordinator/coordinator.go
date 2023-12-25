package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/JyotinderSingh/task-queue/pkg/grpcapi"
	"github.com/google/uuid"

	"github.com/JyotinderSingh/task-queue/pkg/common"
)

const (
	shutdownTimeout  = 5 * time.Second
	defaultMaxMisses = 1
)

type CoordinatorServer struct {
	pb.UnimplementedCoordinatorServiceServer
	serverPort         string
	listener           net.Listener
	grpcServer         *grpc.Server
	WorkerPool         map[uint32]*workerInfo
	WorkerPoolMutex    sync.RWMutex
	maxHeartbeatMisses uint8
	heartbeatInterval  time.Duration
	roundRobinIndex    uint32
	TaskStatus         map[string]pb.TaskStatus
	taskStatusMutex    sync.RWMutex
	ctx                context.Context    // The root context for all goroutines
	cancel             context.CancelFunc // Function to cancel the context
	wg                 sync.WaitGroup     // WaitGroup to wait for all goroutines to finish
}

type workerInfo struct {
	heartbeatMisses     uint8
	address             string
	grpcConnection      *grpc.ClientConn
	workerServiceClient pb.WorkerServiceClient
}

// NewServer initializes and returns a new Server instance.
func NewServer(port string) *CoordinatorServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &CoordinatorServer{
		WorkerPool:         make(map[uint32]*workerInfo),
		TaskStatus:         make(map[string]pb.TaskStatus),
		maxHeartbeatMisses: defaultMaxMisses,
		heartbeatInterval:  common.DefaultHeartbeat,
		serverPort:         port,
		ctx:                ctx,
		cancel:             cancel,
	}
}

// Start initiates the server's operations.
func (s *CoordinatorServer) Start() error {
	go s.manageWorkerPool()

	if err := s.startGRPCServer(); err != nil {
		return fmt.Errorf("gRPC server start failed: %w", err)
	}

	return s.awaitShutdown()
}

func (s *CoordinatorServer) startGRPCServer() error {
	var err error
	s.listener, err = net.Listen("tcp", s.serverPort)
	if err != nil {
		return err
	}

	log.Printf("Starting gRPC server on %s\n", s.serverPort)
	s.grpcServer = grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(s.grpcServer, s)

	go func() {
		if err := s.grpcServer.Serve(s.listener); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	return nil
}

func (s *CoordinatorServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	return s.Stop()
}

// Stop gracefully shuts down the server.
func (s *CoordinatorServer) Stop() error {
	// Signal all goroutines to stop
	s.cancel()
	// Wait for all goroutines to finish
	s.wg.Wait()

	s.WorkerPoolMutex.RLock()
	defer s.WorkerPoolMutex.RUnlock()
	for _, worker := range s.WorkerPool {
		if worker.grpcConnection != nil {
			worker.grpcConnection.Close()
		}
	}

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *CoordinatorServer) SubmitTask(ctx context.Context, in *pb.ClientTaskRequest) (*pb.ClientTaskResponse, error) {
	log.Printf("Called submit task")
	data := in.GetData()
	taskId := uuid.New().String()
	task := &pb.TaskRequest{
		TaskId: taskId,
		Data:   data,
	}

	if err := s.submitTaskToWorker(task); err != nil {
		return nil, err
	}

	s.taskStatusMutex.Lock()
	defer s.taskStatusMutex.Unlock()
	s.TaskStatus[taskId] = pb.TaskStatus_QUEUED

	return &pb.ClientTaskResponse{
		Message: "Task submitted successfully",
		TaskId:  taskId,
	}, nil
}

func (s *CoordinatorServer) UpdateTaskStatus(ctx context.Context, req *pb.UpdateTaskStatusRequest) (*pb.UpdateTaskStatusResponse, error) {
	// Update the task status in your data store here.
	// This is just a placeholder implementation.
	status := req.GetStatus()
	taskId := req.GetTaskId()

	s.taskStatusMutex.Lock()
	defer s.taskStatusMutex.Unlock()
	s.TaskStatus[taskId] = status

	return &pb.UpdateTaskStatusResponse{Success: true}, nil
}

func (s *CoordinatorServer) getNextWorker() *workerInfo {
	s.WorkerPoolMutex.RLock()
	defer s.WorkerPoolMutex.RUnlock()

	workerCount := len(s.WorkerPool)
	if workerCount == 0 {
		return nil
	}

	keys := make([]uint32, 0, workerCount)
	for k := range s.WorkerPool {
		keys = append(keys, k)
	}

	worker := s.WorkerPool[keys[s.roundRobinIndex%uint32(workerCount)]]
	s.roundRobinIndex++
	return worker
}

func (s *CoordinatorServer) GetTaskStatus(ctx context.Context, in *pb.GetTaskStatusRequest) (*pb.GetTaskStatusResponse, error) {
	taskId := in.GetTaskId()
	s.taskStatusMutex.RLock()
	defer s.taskStatusMutex.RUnlock()

	return &pb.GetTaskStatusResponse{
		TaskId: taskId,
		Status: s.TaskStatus[taskId],
	}, nil
}

func (s *CoordinatorServer) submitTaskToWorker(task *pb.TaskRequest) error {
	worker := s.getNextWorker()
	if worker == nil {
		return errors.New("no workers available")
	}

	_, err := worker.workerServiceClient.SubmitTask(context.Background(), task)
	return err
}

func (s *CoordinatorServer) SendHeartbeat(ctx context.Context, in *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.WorkerPoolMutex.Lock()
	defer s.WorkerPoolMutex.Unlock()

	workerID := in.GetWorkerId()

	if worker, ok := s.WorkerPool[workerID]; ok {
		// log.Println("Reset hearbeat miss for worker:", workerID)
		worker.heartbeatMisses = 0
	} else {
		log.Println("Registering worker:", workerID)
		conn, err := grpc.Dial(in.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}

		s.WorkerPool[workerID] = &workerInfo{
			address:             in.GetAddress(),
			grpcConnection:      conn,
			workerServiceClient: pb.NewWorkerServiceClient(conn),
		}
		log.Println("Registered worker:", workerID)
	}

	return &pb.HeartbeatResponse{Acknowledged: true}, nil
}

func (s *CoordinatorServer) manageWorkerPool() {
	s.wg.Add(1)
	defer s.wg.Done()

	ticker := time.NewTicker(time.Duration(s.maxHeartbeatMisses) * s.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.removeInactiveWorkers()
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *CoordinatorServer) removeInactiveWorkers() {
	s.WorkerPoolMutex.Lock()
	defer s.WorkerPoolMutex.Unlock()

	for workerID, worker := range s.WorkerPool {
		if worker.heartbeatMisses > s.maxHeartbeatMisses {
			log.Printf("Removing inactive worker: %d\n", workerID)
			worker.grpcConnection.Close()
			delete(s.WorkerPool, workerID)
		} else {
			worker.heartbeatMisses++
		}
	}
}
