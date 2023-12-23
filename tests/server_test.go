package tests

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	pb "github.com/JyotinderSingh/task-queue/pkg/grpcapi"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

var cluster Cluster
var conn *grpc.ClientConn
var client pb.CoordinatorServiceClient

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

func setup() {
	cluster = Cluster{}
	cluster.LaunchCluster(":50050", 2)

	conn, client = CreateTestClient("localhost:50050")
}

func teardown() {
	cluster.StopCluster()
}

func TestServerIntegration(t *testing.T) {
	assertion := assert.New(t)

	// time.Sleep(10 * time.Second)
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

	err = WaitForCondition(func() bool {
		statusResp, err = client.GetTaskStatus(context.Background(), &pb.GetTaskStatusRequest{TaskId: taskId})
		if err != nil {
			log.Fatalf("Failed to get task status: %v", err)
		}
		return statusResp.GetStatus() == pb.TaskStatus_COMPLETE
	}, 10*time.Second)

	if err != nil {
		t.Fatalf("Task did not complete within the timeout: %v", err)
	}
}
