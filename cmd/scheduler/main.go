package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/JyotinderSingh/task-queue/pkg/scheduler"
)

var (
	schedulerPort = flag.String("scheduler_port", ":8081", "Port on which the Scheduler serves requests.")
)

func main() {
	var missingEnvVars []string

	checkEnvVar := func(envVar, envVarName string) {
		if envVar == "" {
			missingEnvVars = append(missingEnvVars, envVarName)
		}
	}

	dbUser := os.Getenv("POSTGRES_USER")
	checkEnvVar(dbUser, "POSTGRES_USER")

	dbPassword := os.Getenv("POSTGRES_PASSWORD")
	checkEnvVar(dbPassword, "POSTGRES_PASSWORD")

	dbName := os.Getenv("POSTGRES_DB")
	checkEnvVar(dbName, "POSTGRES_DB")

	dbHost := os.Getenv("POSTGRES_HOST")
	if dbHost == "" {
		dbHost = "localhost"
	}

	if len(missingEnvVars) > 0 {
		log.Fatalf("The following required environment variables are not set: %s",
			strings.Join(missingEnvVars, ", "))
	}

	dbConnectionString := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", dbUser, dbPassword, dbHost, dbName)
	schedulerServer := scheduler.NewServer(*schedulerPort, dbConnectionString)
	err := schedulerServer.Start()
	if err != nil {
		log.Fatalf("Error while starting server: %+v", err)
	}
}
