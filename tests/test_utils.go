package tests

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/JyotinderSingh/task-queue/pkg/coordinator"
	pb "github.com/JyotinderSingh/task-queue/pkg/grpcapi"
	"github.com/JyotinderSingh/task-queue/pkg/scheduler"
	"github.com/JyotinderSingh/task-queue/pkg/worker"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	postgresUser     = "postgres"
	postgresPassword = "postgres"
	postgresDb       = "scheduler"
	postgresHost     = "localhost"
)

type Cluster struct {
	coordinatorAddress string
	scheduler          *scheduler.SchedulerServer
	coordinator        *coordinator.CoordinatorServer
	workers            []*worker.WorkerServer
	database           testcontainers.Container
}

func (c *Cluster) LaunchCluster(schedulerPort string, coordinatorPort string, numWorkers int8) {
	// Launch database
	if err := c.createDatabase(); err != nil {
		log.Fatalf("Could not launch database container: %+v", err)
	}

	c.coordinatorAddress = "localhost" + coordinatorPort
	c.coordinator = coordinator.NewServer(coordinatorPort, getDbConnectionString())
	startServer(c.coordinator)

	c.scheduler = scheduler.NewServer(schedulerPort, getDbConnectionString())
	startServer(c.scheduler)

	c.workers = make([]*worker.WorkerServer, numWorkers)
	for i := 0; i < int(numWorkers); i++ {
		c.workers[i] = worker.NewServer("", c.coordinatorAddress)
		startServer(c.workers[i])

	}

	c.waitForWorkers()
}

func (c *Cluster) StopCluster() {
	for _, worker := range c.workers {
		if err := worker.Stop(); err != nil {
			log.Printf("Failed to stop worker: %v", err)
		}
	}
	if err := c.coordinator.Stop(); err != nil {
		log.Printf("Failed to stop coordinator: %v", err)
	}

	if err := c.scheduler.Stop(); err != nil {
		log.Printf("Failed to stop scheduler: %v", err)
	}

	c.database.Terminate(context.Background())
}

func startServer(srv interface {
	Start() error
}) {
	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()
}

func (c *Cluster) waitForWorkers() {
	for {
		c.coordinator.WorkerPoolMutex.Lock()

		c.coordinator.WorkerPoolKeysMutex.RLock()
		if len(c.coordinator.WorkerPoolKeys) == len(c.workers) {
			c.coordinator.WorkerPoolKeysMutex.RUnlock()
			c.coordinator.WorkerPoolMutex.Unlock()
			break
		}
		c.coordinator.WorkerPoolKeysMutex.RUnlock()
		c.coordinator.WorkerPoolMutex.Unlock()
		time.Sleep(time.Second)
	}
}

func getDbConnectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:5432/%s",
		postgresUser, postgresPassword, postgresHost, postgresDb)

}

func (c *Cluster) createDatabase() error {
	ctx := context.Background()

	// Define the container request using your custom image
	req := testcontainers.ContainerRequest{
		Image:        "scheduler-postgres", // Use your custom image
		ExposedPorts: []string{"5432:5432/tcp"},
		Env: map[string]string{
			"POSTGRES_PASSWORD": postgresPassword,
			"POSTGRES_USER":     postgresUser,
			"POSTGRES_DB":       postgresDb,
		},
		WaitingFor: wait.ForListeningPort("5432/tcp"),
	}

	// Start the container
	var err error
	c.database, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	return err
}

func CreateTestClient(coordinatorAddress string) (*grpc.ClientConn, pb.CoordinatorServiceClient) {
	conn, err := grpc.Dial(coordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Fatal("Could not create test connection to coordinator")
	}
	return conn, pb.NewCoordinatorServiceClient(conn)
}

func WaitForCondition(condition func() bool, timeout time.Duration, retryInterval time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-timer.C:
			return fmt.Errorf("timeout exceeded")
		case <-ticker.C:
			if condition() {
				return nil
			}
		}
	}
}
