package main

import (
	"flag"

	"github.com/JyotinderSingh/task-queue/pkg/worker"
)

var (
	serverPort      = flag.String("worker_port", ":50051", "Port on which the Worker serves requests.")
	coordinatorPort = flag.String("coordinator", ":50050", "Network address of the Coordinator.")
)

func main() {
	flag.Parse()

	worker := worker.NewServer(*serverPort, *coordinatorPort)
	worker.Start()
}
