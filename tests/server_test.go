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
var w *worker.WorkerServer

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

func setup() {
	s = server.NewServer()
	w = worker.NewServer()

	go func() {
		if err := s.Start(); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	go func() {
		if err := w.Start(); err != nil {
			log.Fatalf("Failed to start worker: %v", err)
		}
	}()

	time.Sleep(2 * time.Second)
}

func teardown() {
	if err := s.Stop(); err != nil {
		log.Printf("Failed to stop server: %v", err)
	}
	if err := w.Stop(); err != nil {
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

	assertion.Equal("Task submitted", string(body), "Unexpected message in response body")
}
