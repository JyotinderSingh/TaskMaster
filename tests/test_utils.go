package tests

import (
	"fmt"
	"log"
	"time"

	"github.com/JyotinderSingh/task-queue/pkg/coordinator"
	pb "github.com/JyotinderSingh/task-queue/pkg/grpcapi"
	"github.com/JyotinderSingh/task-queue/pkg/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Cluster struct {
	coordinatorAddress string
	coordinator        *coordinator.CoordinatorServer
	workers            []*worker.WorkerServer
}

func (c *Cluster) LaunchCluster(coordinatorPort string, numWorkers int8) {
	c.coordinatorAddress = "localhost" + coordinatorPort
	c.coordinator = coordinator.NewServer(coordinatorPort)

	c.workers = make([]*worker.WorkerServer, numWorkers)

	for i := 0; i < int(numWorkers); i++ {
		c.workers[i] = worker.NewServer("", c.coordinatorAddress)
	}

	startServer(c.coordinator)
	for _, worker := range c.workers {
		startServer(worker)
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
		log.Printf("Failed to stop server: %v", err)
	}

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
		c.coordinator.WorkerPoolMutex.RLock()
		if len(c.coordinator.WorkerPool) == len(c.workers) {
			c.coordinator.WorkerPoolMutex.RUnlock()
			break
		}
		c.coordinator.WorkerPoolMutex.RUnlock()
		time.Sleep(time.Second)
	}
}

func CreateTestClient(coordinatorAddress string) (*grpc.ClientConn, pb.CoordinatorServiceClient) {
	conn, err := grpc.Dial(coordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Fatal("Could not create test connection to coordinator")
	}
	return conn, pb.NewCoordinatorServiceClient(conn)
}

func WaitForCondition(condition func() bool, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(500 * time.Millisecond)
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
