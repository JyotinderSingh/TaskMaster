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
	coordinator = server.NewServer()
	w1 = worker.NewServer(":50051")
	w2 = worker.NewServer(":50052")

	go func() {
		if err := coordinator.Start(); err != nil {
			log.Fatalf("Failed to start server : %v", err)
		}
	}()

	go func() {
		if err := w1.Start(); err != nil {
			log.Fatalf("Failed to start worker: %v", err)
		}
	}()

	go func() {
		if err := w2.Start(); err != nil {
			log.Fatalf("Failed to start worker: %v", err)
		}
	}()

	var err error
	conn, err = grpc.Dial("localhost:50050", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Fatal("Could not create test connection to coordinator")
	}

	client = pb.NewCoordinatorServiceClient(conn)

	time.Sleep(5 * time.Second)
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
	assertion := assert.New(t)
	assertion.Equal(len(coordinator.TaskStatus), 0, "There should be no queued tasks at startup")
	submitResp, err := client.SubmitTask(context.Background(), &pb.ClientTaskRequest{Data: "test"})
	taskId := submitResp.GetTaskId()
	assertion.Nil(err)

	statusResp, err := client.GetTaskStatus(context.Background(), &pb.GetTaskStatusRequest{TaskId: taskId})
	assertion.Nil(err)
	assertion.Equal(pb.TaskStatus_PROCESSING, statusResp.GetStatus())

	time.Sleep(5 * time.Second)

	statusResp, err = client.GetTaskStatus(context.Background(), &pb.GetTaskStatusRequest{TaskId: taskId})
	assertion.Nil(err)
	assertion.Equal(pb.TaskStatus_COMPLETE, statusResp.GetStatus())
}
