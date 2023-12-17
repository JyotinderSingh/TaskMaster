package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/JyotinderSingh/task-queue/pkg/grpcapi"
	"github.com/google/uuid"
)

const (
	serverPort        = ":50050"
	httpServerPort    = ":8080"
	grpcServerAddress = "localhost:50051"
	shutdownTimeout   = 5 * time.Second
	defaultMaxMisses  = 2
	defaultHeartbeat  = 5
)

type CoordinatorServer struct {
	pb.UnimplementedCoordinatorServiceServer
	listener           net.Listener
	grpcServer         *grpc.Server
	httpServer         *http.Server
	workerPool         map[uint32]*workerInfo
	mutex              sync.RWMutex
	maxHeartbeatMisses uint8
	heartbeatInterval  uint8
	roundRobinIndex    uint32
}

type workerInfo struct {
	heartbeatMisses     uint8
	address             string
	grpcConnection      *grpc.ClientConn
	workerServiceClient pb.WorkerServiceClient
}

// NewServer initializes and returns a new Server instance.
func NewServer() *CoordinatorServer {
	return &CoordinatorServer{
		workerPool:         make(map[uint32]*workerInfo),
		maxHeartbeatMisses: defaultMaxMisses,
		heartbeatInterval:  defaultHeartbeat,
	}
}

// Start initiates the server's operations.
func (s *CoordinatorServer) Start() error {
	go s.manageWorkerPool()
	if err := s.startHTTPServer(); err != nil {
		return fmt.Errorf("HTTP server start failed: %w", err)
	}

	if err := s.startGRPCServer(); err != nil {
		return fmt.Errorf("gRPC server start failed: %w", err)
	}

	return s.awaitShutdown()
}

func (s *CoordinatorServer) startHTTPServer() error {
	s.httpServer = &http.Server{Addr: httpServerPort, Handler: http.HandlerFunc(s.handleTaskRequest)}

	go func() {
		log.Printf("Starting HTTP server at %s\n", httpServerPort)
		if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	return nil
}

func (s *CoordinatorServer) startGRPCServer() error {
	var err error
	s.listener, err = net.Listen("tcp", serverPort)
	if err != nil {
		return err
	}

	log.Printf("Starting gRPC server on %s\n", serverPort)
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
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			return fmt.Errorf("HTTP server shutdown failed: %w", err)
		}
	}

	s.mutex.Lock()
	for _, worker := range s.workerPool {
		if worker.grpcConnection != nil {
			worker.grpcConnection.Close()
		}
	}
	s.mutex.Unlock()

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	if s.listener != nil {
		return s.listener.Close()
	}

	return nil
}

func (s *CoordinatorServer) handleTaskRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}

	task := &pb.TaskRequest{
		TaskId: uuid.New().String(),
		Data:   string(body),
	}

	if err = s.submitTask(task); err != nil {
		http.Error(w, fmt.Sprintf("Task submission failed: %s", err), http.StatusInternalServerError)
		return
	}

	fmt.Fprintln(w, "Task submitted successfully")
}

func (s *CoordinatorServer) getNextWorker() *workerInfo {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	workerCount := len(s.workerPool)
	if workerCount == 0 {
		return nil
	}

	keys := make([]uint32, 0, workerCount)
	for k := range s.workerPool {
		keys = append(keys, k)
	}

	worker := s.workerPool[keys[s.roundRobinIndex%uint32(workerCount)]]
	s.roundRobinIndex++
	return worker
}

func (s *CoordinatorServer) submitTask(task *pb.TaskRequest) error {
	worker := s.getNextWorker()
	if worker == nil {
		return errors.New("no workers available")
	}

	_, err := worker.workerServiceClient.SubmitTask(context.Background(), task)
	return err
}

func (s *CoordinatorServer) SendHeartbeat(ctx context.Context, in *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	workerID := in.GetWorkerId()

	log.Println("Received heartbeat from worker:", workerID)
	if worker, ok := s.workerPool[workerID]; ok {
		worker.heartbeatMisses = 0
	} else {
		log.Println("Registering worker:", workerID)
		conn, err := grpc.Dial(grpcServerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			return nil, err
		}

		s.workerPool[workerID] = &workerInfo{
			address:             in.GetAddress(),
			grpcConnection:      conn,
			workerServiceClient: pb.NewWorkerServiceClient(conn),
		}
		log.Println("Registered worker:", workerID)
	}

	return &pb.HeartbeatResponse{Acknowledged: true}, nil
}

func (s *CoordinatorServer) manageWorkerPool() {
	ticker := time.NewTicker(time.Duration(s.heartbeatInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.removeInactiveWorkers()
		}
	}
}

func (s *CoordinatorServer) removeInactiveWorkers() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for workerID, worker := range s.workerPool {
		if worker.heartbeatMisses > s.maxHeartbeatMisses {
			log.Printf("Removing inactive worker: %d\n", workerID)
			delete(s.workerPool, workerID)
			worker.grpcConnection.Close()
		} else {
			worker.heartbeatMisses++
		}
	}
}
