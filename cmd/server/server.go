package main

import (
	"flag"

	"github.com/JyotinderSingh/task-queue/pkg/server"
)

var (
	serverPort = flag.String("server_port", ":50050", "Port on which the Coordinator serves requests.")
)

func main() {
	flag.Parse()

	coordinator := server.NewServer(*serverPort)
	coordinator.Start()
}
