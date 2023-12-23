package common

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

const (
	DefaultHeartbeat = 5 * time.Second
)

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
