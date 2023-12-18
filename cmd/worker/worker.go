package main

import (
	"github.com/JyotinderSingh/task-queue/pkg/worker"
	// other necessary imports
)

func main() {
	// Initialize configuration
	// Set up logging, database connections, etc.

	workerServer := worker.NewServer(":50051") // Create a new server instance
	workerServer.Start()
	// Handle graceful shutdown
}
