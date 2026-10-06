package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"dagger.io/dagger"
)

const (
	goImage        = "golang:1.23-alpine"
	postgresImage  = "postgres:17-alpine"
	rabbitmqImage  = "rabbitmq:4-management-alpine"
	mosquittoImage = "eclipse-mosquitto:2"
)

func main() {
	ctx := context.Background()

	// ------------------------------------------------------------
	// Connect to Dagger Engine
	// ------------------------------------------------------------

	client, err := dagger.Connect(
		ctx,
		dagger.WithLogOutput(os.Stderr),
	)
	if err != nil {
		log.Fatalf("failed to instantiate dagger client: %v", err)
	}
	defer client.Close()

	// ------------------------------------------------------------
	// Resolve source directory
	// ------------------------------------------------------------

	dir := "."

	if len(os.Args) > 1 {
		dir = os.Args[1]
	}

	dir, err = filepath.Abs(dir)
	if err != nil {
		log.Fatalf("failed to resolve absolute path of %s: %v", dir, err)
	}

	src := client.Host().Directory(
		dir,
		dagger.HostDirectoryOpts{
			Exclude: []string{
				".git",
				".dagger",
			},
		},
	)

	// ------------------------------------------------------------
	// PostgreSQL
	// ------------------------------------------------------------

	postgres := client.Container().
		From(postgresImage).
		WithEnvVariable("POSTGRES_USER", "fleet").
		WithEnvVariable("POSTGRES_PASSWORD", "fleet").
		WithEnvVariable("POSTGRES_DB", "fleet").
		WithMountedFile("/docker-entrypoint-initdb.d/schema.sql", src.File("internal/postgres/schema.sql")).
		WithMountedFile("/docker-entrypoint-initdb.d/query.sql", src.File("internal/postgres/query.sql")).
		WithExposedPort(5434)

	postgresService := postgres.AsService(
		dagger.ContainerAsServiceOpts{},
	)

	// ------------------------------------------------------------
	// RabbitMQ
	// ------------------------------------------------------------

	rabbitmq := client.Container().
		From(rabbitmqImage).
		WithEnvVariable("RABBITMQ_DEFAULT_USER", "fleet").
		WithEnvVariable("RABBITMQ_DEFAULT_PASS", "fleet").
		WithExposedPort(5672).
		WithExposedPort(15672)

	rabbitmqService := rabbitmq.AsService(
		dagger.ContainerAsServiceOpts{},
	)

	// ------------------------------------------------------------
	// Mosquitto
	// ------------------------------------------------------------

	mosquittoConfig := src.File(
		"deployments/mosquitto.conf",
	)

	mosquitto := client.Container().
		From(mosquittoImage).
		WithFile(
			"/mosquitto/config/mosquitto.conf",
			mosquittoConfig,
		).
		WithExposedPort(1883)

	mosquittoService := mosquitto.AsService(
		dagger.ContainerAsServiceOpts{},
	)


	// ------------------------------------------------------------
	// Test environment
	// ------------------------------------------------------------

	test := client.Container().
		From(goImage).
		WithMountedDirectory("/src", src).
		WithWorkdir("/src").

		// Go module cache
		WithMountedCache(
			"/go/pkg/mod",
			client.CacheVolume("fleet-go-mod"),
		).

		// Go compiler cache
		WithMountedCache(
			"/root/.cache/go-build",
			client.CacheVolume("fleet-go-build"),
		).

		// PostgreSQL
		WithServiceBinding(
			"postgres",
			postgresService,
		).

		// RabbitMQ
		WithServiceBinding(
			"rabbitmq",
			rabbitmqService,
		).

		// Mosquitto
		WithServiceBinding(
			"mosquitto",
			mosquittoService,
		).

		// Same environment as docker-compose
		WithEnvVariable(
			"LOG_LEVEL",
			"debug",
		).

		WithEnvVariable(
			"DB_CONNECTION",
			"postgres://fleet:fleet@postgres:5432/fleet?sslmode=disable",
		).

		WithEnvVariable(
			"MQTT_BROKER",
			"tcp://mosquitto:1883",
		).

		WithEnvVariable(
			"RABBITMQ_URL",
			"amqp://fleet:fleet@rabbitmq:5672/",
		).

		WithEnvVariable(
			"RABBITMQ_EXCHANGE",
			"fleet.events",
		).

		WithEnvVariable(
			"RABBITMQ_QUEUE",
			"geofence_alerts",
		).

		WithEnvVariable(
			"RABBITMQ_ROUTING_KEY",
			"geofence.entry",
		)

	test = test.WithExec([]string{
		"sh",
		"-c",
		`
		set -eu

		echo "Waiting for PostgreSQL..."
		until nc -z postgres 5432; do
			echo "PostgreSQL is not ready"
			sleep 1
		done

		echo "Waiting for RabbitMQ..."
		until nc -z rabbitmq 5672; do
			echo "RabbitMQ is not ready"
			sleep 1
		done

		echo "Waiting for Mosquitto..."
		until nc -z mosquitto 1883; do
			echo "Mosquitto is not ready"
			sleep 1
		done

		echo "All infrastructure services are ready."
	`,
		})

	// ------------------------------------------------------------
	// Go test
	// ------------------------------------------------------------

	test = test.WithExec([]string{
		"go",
		"test",
		"-coverprofile=coverage.out",
		"./...",
	})

	_, err = test.
		File("coverage.out").
		Export(
			ctx,
			filepath.Join(dir, "coverage.out"),
		)

	if err != nil {
		log.Fatalf("failed to export coverage: %v", err)
	}

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println(" Dagger test completed successfully")
	fmt.Println(" Coverage: coverage.out")
	fmt.Println("========================================")
}