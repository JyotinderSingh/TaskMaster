package tests

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	pb "github.com/JyotinderSingh/task-queue/pkg/grpcapi"
	"github.com/JyotinderSingh/task-queue/pkg/server"
	"github.com/JyotinderSingh/task-queue/pkg/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var coordinator *server.CoordinatorServer
var w1 *worker.WorkerServer
var w2 *worker.WorkerServer
var conn *grpc.ClientConn
var client pb.CoordinatorServiceClient

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

func setup() {
	coordinator = server.NewServer(":50050")
	w1 = worker.NewServer(":50051")
	w2 = worker.NewServer(":50052")

	startServers()
	createClientConnection()
}

func startServers() {
	startServer(coordinator)
	startServer(w1)
	startServer(w2)

	// Replace with a dynamic check to ensure servers are ready
	time.Sleep(10 * time.Second)
}

func startServer(srv interface {
	Start() error
}) {
	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()
}

func createClientConnection() {
	var err error
	conn, err = grpc.Dial("localhost:50050", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Fatal("Could not create test connection to coordinator")
	}
	client = pb.NewCoordinatorServiceClient(conn)
}

func teardown() {
	if err := coordinator.Stop(); err != nil {
		log.Printf("Failed to stop server: %v", err)
	}
	if err := w1.Stop(); err != nil {
		log.Printf("Failed to stop worker: %v", err)
	}
	if err := w2.Stop(); err != nil {
		log.Printf("Failed to stop worker: %v", err)
	}
}

func TestServerIntegration(t *testing.T) {
	t.Parallel() // Allow for parallel test execution where appropriate

	assertion := assert.New(t)

	submitResp, err := client.SubmitTask(context.Background(), &pb.ClientTaskRequest{Data: "test"})
	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}
	taskId := submitResp.GetTaskId()

	statusResp, err := client.GetTaskStatus(context.Background(), &pb.GetTaskStatusRequest{TaskId: taskId})
	if err != nil {
		t.Fatalf("Failed to get task status: %v", err)
	}
	assertion.Equal(pb.TaskStatus_PROCESSING, statusResp.GetStatus())

	// Replace with a dynamic check to wait for task completion
	time.Sleep(7 * time.Second)

	statusResp, err = client.GetTaskStatus(context.Background(), &pb.GetTaskStatusRequest{TaskId: taskId})
	if err != nil {
		t.Fatalf("Failed to get task status: %v", err)
	}
	assertion.Equal(pb.TaskStatus_COMPLETE, statusResp.GetStatus())
}
