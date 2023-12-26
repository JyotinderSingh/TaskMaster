package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/JyotinderSingh/task-queue/pkg/common"
	"github.com/jackc/pgx/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
)

// CommandRequest represents the structure of the request body
type CommandRequest struct {
	Command     string `json:"command"`
	ScheduledAt string `json:"scheduled_at"` // ISO 8601 format
}

type Task struct {
	Id          string
	Command     string
	ScheduledAt pgtype.Timestamp
	PickedAt    pgtype.Timestamp
	StartedAt   pgtype.Timestamp
	CompletedAt pgtype.Timestamp
	FailedAt    pgtype.Timestamp
}

// SchedulerServer represents an HTTP server that manages tasks.
type SchedulerServer struct {
	serverPort         string
	dbConnectionString string
	dbPool             *pgxpool.Pool
	ctx                context.Context
	cancel             context.CancelFunc
	httpServer         *http.Server
}

// NewServer creates and returns a new SchedulerServer.
func NewServer(port string, dbConnectionString string) *SchedulerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &SchedulerServer{
		serverPort:         port,
		dbConnectionString: dbConnectionString,
		ctx:                ctx,
		cancel:             cancel,
	}
}

// Start initializes and starts the SchedulerServer.
func (s *SchedulerServer) Start() error {
	var err error
	s.dbPool, err = common.ConnectToDatabase(s.ctx, s.dbConnectionString)
	if err != nil {
		return err
	}

	http.HandleFunc("/schedule", s.handleScheduleTask)
	http.HandleFunc("/status/", s.handleGetTaskStatus) // Add the new route handler

	s.httpServer = &http.Server{
		Addr: s.serverPort,
	}

	log.Printf("Starting scheduler server on %s\n", s.serverPort)

	// Start the server in a separate goroutine
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %s\n", err)
		}
	}()

	// Return awaitShutdown
	return s.awaitShutdown()
}

// handleScheduleTask handles POST requests to add new tasks.
func (s *SchedulerServer) handleScheduleTask(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// Decode the JSON body
	var commandReq CommandRequest
	if err := json.NewDecoder(r.Body).Decode(&commandReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Received schedule request: %+v", commandReq)

	// Parse the scheduled_at time
	scheduledTime, err := time.Parse(time.RFC3339, commandReq.ScheduledAt)
	if err != nil {
		http.Error(w, "Invalid date format. Use ISO 8601 format.", http.StatusBadRequest)
		return
	}

	// Convert the scheduled time to Unix timestamp
	unixTimestamp := time.Unix(scheduledTime.Unix(), 0)

	taskId, err := s.insertTaskIntoDB(context.Background(), Task{Command: commandReq.Command, ScheduledAt: pgtype.Timestamp{Time: unixTimestamp}})

	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to submit task. Error: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}

	// Respond with the parsed data (for demonstration purposes)
	response := struct {
		Command     string `json:"command"`
		ScheduledAt int64  `json:"scheduled_at"`
		TaskID      string `json:"task_id"`
	}{
		Command:     commandReq.Command,
		ScheduledAt: unixTimestamp.Unix(),
		TaskID:      taskId,
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResponse)
}

func (s *SchedulerServer) handleGetTaskStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get the task ID from the query parameters
	taskID := r.URL.Query().Get("task_id")

	// Check if the task ID is empty
	if taskID == "" {
		http.Error(w, "Task ID is required", http.StatusBadRequest)
		return
	}

	// Query the database to get the task status
	var task Task
	err := s.dbPool.QueryRow(context.Background(), "SELECT * FROM tasks WHERE id = $1", taskID).Scan(&task.Id, &task.Command, &task.ScheduledAt, &task.PickedAt, &task.StartedAt, &task.CompletedAt, &task.FailedAt)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get task status. Error: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	// Prepare the response JSON
	response := struct {
		TaskID      string `json:"task_id"`
		Command     string `json:"command"`
		ScheduledAt string `json:"scheduled_at,omitempty"`
		PickedAt    string `json:"picked_at,omitempty"`
		StartedAt   string `json:"started_at,omitempty"`
		CompletedAt string `json:"completed_at,omitempty"`
		FailedAt    string `json:"failed_at,omitempty"`
	}{
		TaskID:      task.Id,
		Command:     task.Command,
		ScheduledAt: "",
		PickedAt:    "",
		StartedAt:   "",
		CompletedAt: "",
		FailedAt:    "",
	}

	// Set the scheduled_at time if non-null.
	if task.ScheduledAt.Status == 2 {
		response.ScheduledAt = task.ScheduledAt.Time.String()
	}

	// Set the picked_at time if non-null.
	if task.PickedAt.Status == 2 {
		response.PickedAt = task.PickedAt.Time.String()
	}

	// Set the started_at time if non-null.
	if task.StartedAt.Status == 2 {
		response.StartedAt = task.StartedAt.Time.String()
	}

	// Set the completed_at time if non-null.
	if task.CompletedAt.Status == 2 {
		response.CompletedAt = task.CompletedAt.Time.String()
	}

	// Set the failed_at time if non-null.
	if task.FailedAt.Status == 2 {
		response.FailedAt = task.FailedAt.Time.String()
	}

	// Convert the response struct to JSON
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "Failed to marshal JSON response", http.StatusInternalServerError)
		return
	}

	// Set the Content-Type header to application/json
	w.Header().Set("Content-Type", "application/json")

	// Write the JSON response
	w.Write(jsonResponse)
}

// insertTaskIntoDB inserts a new task into the tasks table and returns the autogenerated UUID.
func (s *SchedulerServer) insertTaskIntoDB(ctx context.Context, task Task) (string, error) {
	// SQL statement with RETURNING clause
	sqlStatement := "INSERT INTO tasks (command, scheduled_at) VALUES ($1, $2) RETURNING id"

	var insertedId string

	// Execute the query and scan the returned id into the insertedId variable
	err := s.dbPool.QueryRow(ctx, sqlStatement, task.Command, task.ScheduledAt.Time).Scan(&insertedId)
	if err != nil {
		return "", err
	}

	// Return the autogenerated UUID
	return insertedId, nil
}

func (s *SchedulerServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	return s.Stop()
}

// Stop gracefully shuts down the SchedulerServer and the database connection pool.
func (s *SchedulerServer) Stop() error {
	s.dbPool.Close()
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.httpServer.Shutdown(ctx)
	}
	log.Println("Scheduler server and database pool stopped")
	return nil
}
