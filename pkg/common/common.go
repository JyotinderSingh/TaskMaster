package common

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

const (
	DefaultHeartbeat = 5 * time.Second
)

func GetDBConnectionString() string {
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

	return fmt.Sprintf("postgres://%s:%s@%s:5432/%s", dbUser, dbPassword, dbHost, dbName)
}

func ConnectToDatabase(ctx context.Context, dbConnectionString string) (*pgxpool.Pool, error) {
	var dbPool *pgxpool.Pool
	var err error
	retryCount := 0
	for retryCount < 5 {
		dbPool, err = pgxpool.Connect(ctx, dbConnectionString)
		if err == nil {
			break
		}
		log.Printf("Failed to connect to the database. Retrying in 5 seconds...")
		time.Sleep(5 * time.Second)
		retryCount++
	}

	if err != nil {
		log.Printf("Ran out of retries to connect to database (5)")
		return nil, err
	}

	log.Printf("Connected to the database.")
	return dbPool, nil
}
