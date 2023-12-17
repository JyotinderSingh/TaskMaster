package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/JyotinderSingh/task-queue/pkg/grpcapi"
	"github.com/google/uuid"
)

// Server represents the server structure with necessary fields.
type Server struct {
	grpcConnection      *grpc.ClientConn
	workerServiceClient pb.WorkerServiceClient
	httpServer          *http.Server
}

// NewServer initializes and returns a new Server instance.
func NewServer() *Server {
	return &Server{}
}

// Start initiates the server's operations.
func (s *Server) Start() error {
	if err := s.setupGRPCConnection(); err != nil {
		return err
	}
	defer s.grpcConnection.Close()

	if err := s.startHTTPServer(); err != nil {
		return err
	}

	return s.awaitShutdown()
}

// setupGRPCConnection establishes a connection to the gRPC server.
func (s *Server) setupGRPCConnection() error {
	log.Println("Connecting to worker...")
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("failed to connect to worker: %w", err)
	}

	s.grpcConnection = conn
	s.workerServiceClient = pb.NewWorkerServiceClient(s.grpcConnection)
	log.Println("Connected to worker!")
	return nil
}

// startHTTPServer starts the HTTP server to handle incoming requests.
func (s *Server) startHTTPServer() error {
	s.httpServer = &http.Server{Addr: ":8080", Handler: nil}
	http.HandleFunc("/", s.handleTaskRequest)

	errChan := make(chan error, 1)
	go func() {
		log.Println("Starting HTTP server at :8080")
		if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			errChan <- fmt.Errorf("failed to start HTTP server: %w", err)
		}
	}()

	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

// awaitShutdown waits for termination signals and gracefully shuts down the server.
func (s *Server) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop
	return s.shutdownHTTPServer()
}

// shutdownHTTPServer handles the graceful shutdown of the HTTP server.
func (s *Server) shutdownHTTPServer() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to gracefully shutdown the server: %w", err)
	}
	return nil
}

// handleTaskRequest processes the HTTP request for task submission.
func (s *Server) handleTaskRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	task := &pb.TaskRequest{
		TaskId: uuid.New().String(),
		Data:   string(body),
	}

	if err = s.submitTask(task); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Task submitted")
}

// submitTask performs the gRPC call to send a task to the worker.
func (s *Server) submitTask(task *pb.TaskRequest) error {
	_, err := s.workerServiceClient.SubmitTask(context.Background(), task)
	if err != nil {
		return fmt.Errorf("task could not be submitted: %w", err)
	}
	return nil
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() error {
	if s.httpServer != nil {
		if err := s.shutdownHTTPServer(); err != nil {
			return err
		}
	}
	if s.grpcConnection != nil {
		s.grpcConnection.Close()
	}
	return nil
}
