package main

import (
	"flag"

	"github.com/JyotinderSingh/task-queue/pkg/coordinator"
)

var (
	coordinatorPort = flag.String("coordinator_port", ":8080", "Port on which the Coordinator serves requests.")
)

func main() {
	flag.Parse()

	coordinator := coordinator.NewServer(*coordinatorPort)
	coordinator.Start()
}
