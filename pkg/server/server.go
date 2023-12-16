package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/JyotinderSingh/task-queue/pkg/grpcapi"
)

type server struct {
	grpcConnection      *grpc.ClientConn
	workerServiceClient pb.WorkerServiceClient
}

func (s *server) sendTaskToWorker(c pb.WorkerServiceClient, data string) {
	_, err := c.SendTask(context.Background(), &pb.TaskRequest{Data: data})
	if err != nil {
		log.Fatalf("could not send task: %v", err)
	}
}

func (s *server) handleTaskRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusInternalServerError)
		}
		go s.sendTaskToWorker(s.workerServiceClient, string(body))
		fmt.Fprintf(w, "Task sent to worker")
	} else {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
	}
}

func (s *server) connectToWorker() {
	// Create a connection to the worker server.
	conn, err := grpc.Dial("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	s.grpcConnection = conn

	s.workerServiceClient = pb.NewWorkerServiceClient(s.grpcConnection)
}

func (s *server) Start() {
	fmt.Println("Connecting to worker...")
	s.connectToWorker()
	defer s.grpcConnection.Close()
	fmt.Println("Connected to worker!")

	http.HandleFunc("/", s.handleTaskRequest)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func (s *server) Stop() {
}

func NewServer() server {
	return server{}
}
