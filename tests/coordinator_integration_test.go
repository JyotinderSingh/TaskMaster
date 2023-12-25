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

func setup(numWorkers int8) {
	cluster = Cluster{}
	cluster.LaunchCluster(":8081", ":50050", numWorkers)

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
	setup(2)
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
	setup(2)
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

func TestCoordinatorFailoverForInactiveWorkers(t *testing.T) {
	setup(2)
	defer teardown()

	// Stop one worker in the cluster.
	cluster.workers[0].Stop()

	err := WaitForCondition(func() bool {
		cluster.coordinator.WorkerPoolMutex.Lock()
		numWorkers := len(cluster.coordinator.WorkerPool)
		cluster.coordinator.WorkerPoolMutex.Unlock()
		return numWorkers == 1
	}, 20*time.Second, common.DefaultHeartbeat)

	if err != nil {
		log.Fatalf("Coordinator did not clean up inactive workers.")
	}

	for i := 0; i < 4; i++ {
		_, err := client.SubmitTask(context.Background(), &pb.ClientTaskRequest{Data: "test"})
		if err != nil {
			t.Fatalf("Failed to submit task: %v", err)
		}
	}

	err = WaitForCondition(func() bool {
		worker := cluster.workers[1]
		worker.ReceivedTasksMutex.Lock()
		if len(worker.ReceivedTasks) != 4 {
			worker.ReceivedTasksMutex.Unlock()
			return false
		}
		worker.ReceivedTasksMutex.Unlock()

		return true
	}, 10*time.Second, 500*time.Millisecond)

	if err != nil {
		log.Fatalf("Coordinator not routing requests correctly after failover.")
	}
}

func TestTaskLoadBalancingOverWorkers(t *testing.T) {
	setup(4)
	defer teardown()

	for i := 0; i < 8; i++ {
		_, err := client.SubmitTask(context.Background(), &pb.ClientTaskRequest{Data: "test"})
		if err != nil {
			t.Fatalf("Failed to submit task: %v", err)
		}
	}

	err := WaitForCondition(func() bool {
		for _, worker := range cluster.workers {
			worker.ReceivedTasksMutex.Lock()
			if len(worker.ReceivedTasks) != 2 {
				worker.ReceivedTasksMutex.Unlock()
				return false
			}
			worker.ReceivedTasksMutex.Unlock()
		}
		return true
	}, 5*time.Second, 500*time.Millisecond)

	if err != nil {
		for idx, worker := range cluster.workers {
			worker.ReceivedTasksMutex.Lock()
			log.Printf("Worker %d has %d tasks in its log", idx, len(worker.ReceivedTasks))

			worker.ReceivedTasksMutex.Unlock()
		}
		t.Fatalf("Coordinator is not using round-robin to execute tasks over worker pool.")
	}
}

func TestDBConnection(t *testing.T) {
	setup(4)

}
