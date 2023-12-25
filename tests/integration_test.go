package tests

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/JyotinderSingh/task-queue/pkg/common"
	pb "github.com/JyotinderSingh/task-queue/pkg/grpcapi"
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

	// Send POST request
	postData := map[string]interface{}{
		"command":      "yoooo",
		"scheduled_at": "2023-12-24T22:34:00+05:30",
	}
	postDataBytes, _ := json.Marshal(postData)
	resp, err := http.Post("http://localhost:8081/schedule", "application/json", strings.NewReader(string(postDataBytes)))
	if err != nil {
		t.Fatalf("Failed to send POST request: %v", err)
	}
	defer resp.Body.Close()

	// Parse response JSON
	var postResponse map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&postResponse)
	taskID := postResponse["task_id"].(string)

	// Wait for the response to contain the keys "picked_at", "started_at", and "completed_at"
	err = WaitForCondition(func() bool {
		getURL := "http://localhost:8081/status?task_id=" + url.QueryEscape(taskID)
		resp, err := http.Get(getURL)
		if err != nil {
			log.Fatalf("Failed to send GET request: %v", err)
		}
		defer resp.Body.Close()

		// Parse response JSON
		var getResponse map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&getResponse)

		// Check if the response contains the keys "picked_at", "started_at", and "completed_at"
		_, pickedAtExists := getResponse["picked_at"]
		_, startedAtExists := getResponse["started_at"]
		_, completedAtExists := getResponse["completed_at"]
		return pickedAtExists && startedAtExists && completedAtExists
	}, 20*time.Second, 1*time.Second)

	if err != nil {
		t.Fatalf("Response did not contain the keys 'picked_at', 'started_at', and 'completed_at': %v", err)
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