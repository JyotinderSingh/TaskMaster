package tests

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/JyotinderSingh/task-queue/pkg/common"
	pb "github.com/JyotinderSingh/task-queue/pkg/grpcapi"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

var cluster Cluster
var conn *grpc.ClientConn
var client pb.CoordinatorServiceClient

func setup() {
	cluster = Cluster{}
	cluster.LaunchCluster(":50050", 2)

	conn, client = CreateTestClient("localhost:50050")
}

func teardown() {
	cluster.StopCluster()
}

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

func TestE2ESuccess(t *testing.T) {
	setup()
	defer teardown()

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

	err = WaitForCondition(func() bool {
		statusResp, err = client.GetTaskStatus(context.Background(), &pb.GetTaskStatusRequest{TaskId: taskId})
		if err != nil {
			log.Fatalf("Failed to get task status: %v", err)
		}
		return statusResp.GetStatus() == pb.TaskStatus_COMPLETE
	}, 10*time.Second, 500*time.Millisecond)

	if err != nil {
		t.Fatalf("Task did not complete within the timeout: %v", err)
	}
}

func TestWorkersNotAvailable(t *testing.T) {
	setup()
	defer teardown()

	for _, worker := range cluster.workers {
		worker.Stop()
	}

	err := WaitForCondition(func() bool {
		_, err := client.SubmitTask(context.Background(), &pb.ClientTaskRequest{Data: "test"})
		return err != nil && err.Error() == "rpc error: code = Unknown desc = no workers available"
	}, 20*time.Second, common.DefaultHeartbeat)

	if err != nil {
		t.Fatalf("Coordinator did not clean up the workers within SLO. Error: %s", err.Error())
	}
}
