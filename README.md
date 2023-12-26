# TaskMaster: Task Scheduler in Go

![TaskMaster Hero](assets/lightmode.png#gh-light-mode-only)
![TaskMaster Hero](assets/darkmode.png#gh-dark-mode-only)

TaskMaster is a robust and efficient task scheduler written in Go. It's designed to handle a high volume of tasks, distribute them across multiple workers for execution.

## System Components

TaskMaster is composed of several key components that work together to schedule and execute tasks. Here's a brief overview of each component:

- **Scheduler**: The scheduler is the front-end server of the system. It receives tasks from the clients and schedules them for execution.

- **Coordinator**: The coordinator is responsible for selecting the tasks that need to be executed at a given instant based on their schedules. It selects the tasks to be executed and distributeds them across the available workers to execute.

- **Worker**: Workers are responsible for executing the tasks assigned to them by the coordinator. Once a task is completed, the worker reports the status back to the coordinator.

- **Client**: Clients submit tasks to the scheduler for execution using an HTTP endpoint. They can also query the scheduler for the status of their tasks.

- **Database**: A PostgreSQL database is used to store information about tasks such as their ID, task information, scheduling information, completion information. The coordinator and scheduler interact with the database to retrieve and update task information.

Each of these components is implemented as a separate service, and they communicate with each other using gRPC. This architecture allows for high scalability and fault tolerance.

## Directory Structure

Here's a brief overview of the project's directory structure:

- [`cmd/`](./cmd/): Contains the main entry points for the scheduler, coordinator, and worker services.
- [`pkg/`](./pkg/): Contains the core logic for the scheduler, coordinator, and worker services.
- [`data/`](./data/): Contains SQL scripts to initialize the db.
- [`tests/`](./tests/): Contains integration tests.
- [`*-dockerfile`](./docker-compose.yml): Dockerfiles for building the scheduler, coordinator, and worker services.
- [`docker-compose.yml`](./docker-compose.yml): Docker Compose configuration file for spinning up the entire cluster.

## Spinning Up a Cluster

To spin up a complete cluster using Docker Compose, run the following command:

```sh
docker-compose up --build --scale worker=3
```

This command builds the Docker images for the coordinator, scheduler, and worker services, and then starts up a cluster with one coordinator, one scheduler, and three workers. The `--scale` option allows you to specify the number of workers.

Please note that you need to have Docker and Docker Compose installed on your machine to run this command.

## Interacting with the Cluster

Clients can interact with the TaskMaster cluster to schedule tasks and query their status using HTTP requests:

### Scheduling a Task

To schedule a task, send a POST request to `localhost:8081/schedule` with a JSON body including:

- `"command"`: A string representing the command to be executed.
- `"scheduled_at"`: A string representing the current time in ISO 8601 format.

This request returns a response including the `"task_id"`, which can be used to query the status of the task.

#### Example Schedule Request

```sh
curl -X POST localhost:8081/schedule -d '{"command":"<your-command>","scheduled_at":"2023-12-25T22:34:00+05:30"}'
```

### Querying Task Status

To get the status of a scheduled task, send a GET request to `localhost:8081/status?task_id=<task-id>` where `<task-id>` is the ID of the task returned by the schedule request.

#### Example Get Status Request

```sh
curl localhost:8081/status?task_id=<task-id>
```
