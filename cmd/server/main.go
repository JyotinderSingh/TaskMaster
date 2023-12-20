package main

import (
	"flag"

	"github.com/JyotinderSingh/task-queue/pkg/server"
)

var (
	serverPort = flag.String("server_port", ":8080", "Port on which the Coordinator serves requests.")
)

func main() {
	flag.Parse()

	coordinator := server.NewServer(*serverPort)
	coordinator.Start()
}
