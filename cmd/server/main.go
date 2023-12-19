package main

import (
	"fmt"

	"github.com/JyotinderSingh/task-queue/pkg/server"
	// other necessary imports
)

func main() {
	// Initialize configuration
	// Set up logging, database connections, etc.

	srv := server.NewServer(":50050") // Create a new server instance
	fmt.Println("Starting server")
	srv.Start() // Start the server
}
