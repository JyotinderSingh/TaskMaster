package main

import (
	"flag"
	"log"

	"github.com/JyotinderSingh/task-queue/pkg/common"
	"github.com/JyotinderSingh/task-queue/pkg/scheduler"
)

var (
	schedulerPort = flag.String("scheduler_port", ":8081", "Port on which the Scheduler serves requests.")
)

func main() {
	dbConnectionString := common.GetDBConnectionString()
	schedulerServer := scheduler.NewServer(*schedulerPort, dbConnectionString)
	err := schedulerServer.Start()
	if err != nil {
		log.Fatalf("Error while starting server: %+v", err)
	}
}
