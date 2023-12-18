package tests

import (
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/JyotinderSingh/task-queue/pkg/server"
	"github.com/JyotinderSingh/task-queue/pkg/worker"
)

var s *server.CoordinatorServer
var w1 *worker.WorkerServer
var w2 *worker.WorkerServer

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

func setup() {
	s = server.NewServer()
	w1 = worker.NewServer(":50051")
	w2 = worker.NewServer(":50052")

	go func() {
		if err := s.Start(); err != nil {
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

	time.Sleep(10 * time.Second)
}

func teardown() {
	if err := s.Stop(); err != nil {
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

	resp, err := http.Post("http://localhost:8080/task", "application/json", strings.NewReader(`{"data": "test task"}`))
	assertion.NoError(err, "Failed to send task to worker")
	defer resp.Body.Close()

	assertion.Equal(http.StatusOK, resp.StatusCode, "Expected status OK, got a different status")

	body, err := io.ReadAll(resp.Body)
	assertion.NoError(err, "Failed to read response body")

	assertion.Equal("Task submitted successfully\n", string(body), "Unexpected message in response body")
}
