package cmd

import (
	"context"
	"strings"
	"testing"
)

func TestRedactURL(t *testing.T) {
	tests := []struct{ in, want string }{
		{"postgres://fleet:secret@postgres:5432/fleet?sslmode=disable", "postgres://fleet:xxxxx@postgres:5432/fleet?sslmode=disable"},
		{"postgres://fleet@postgres:5432/fleet", "postgres://fleet@postgres:5432/fleet"},
		{"postgres://postgres:5432/fleet?password=secret", "postgres://postgres:5432/fleet?password=xxxxx"},
		{"host=localhost user=fleet password=secret", "xxxxx"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := redactURL(tt.in); got != tt.want {
			t.Errorf("redactURL(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestLogValueHidesPassword(t *testing.T) {
	c := CMD{
		PostgresConfig: PostgresConfig{DBConnection: "postgres://fleet:secret@postgres:5432/fleet"},
		RabbitMQConfig: RabbitMQConfig{RabbitMQURI: "amqp://fleet:secret@rabbitmq:5672/"},
	}
	if s := c.LogValue().String(); strings.Contains(s, "secret") {
		t.Fatalf("log value leaks password: %s", s)
	}
}

func TestNewRequiresDBConnection(t *testing.T) {
	t.Setenv("DB_CONNECTION", "")
	_, err := New(context.Background(), WithoutDotEnv())
	if err == nil || !strings.Contains(err.Error(), "DB_CONNECTION") {
		t.Fatalf("err = %v, want DB_CONNECTION error", err)
	}
}

func TestNewInvalidLogLevel(t *testing.T) {
	t.Setenv("DB_CONNECTION", "postgres://fleet:fleet@localhost:1/fleet")
	t.Setenv("LOG_LEVEL", "verbose")
	_, err := New(context.Background(), WithoutDotEnv())
	if err == nil || !strings.Contains(err.Error(), "LOG_LEVEL") {
		t.Fatalf("err = %v, want LOG_LEVEL error", err)
	}
}

func TestNewWithEnvPrefix(t *testing.T) {
	t.Setenv("FLEET_DB_CONNECTION", "postgres://fleet:fleet@localhost:1/fleet")
	t.Setenv("FLEET_LOG_LEVEL", "verbose")
	// Prefix terbaca bila error berasal dari LOG_LEVEL, bukan DB_CONNECTION kosong.
	_, err := New(context.Background(), WithoutDotEnv(), WithEnvPrefix("FLEET_"))
	if err == nil || !strings.Contains(err.Error(), "LOG_LEVEL") {
		t.Fatalf("err = %v, want LOG_LEVEL error", err)
	}
}

func TestNewInvalidGeofencePoints(t *testing.T) {
	t.Setenv("DB_CONNECTION", "postgres://fleet:fleet@localhost:1/fleet")
	t.Setenv("GEOFENCE_POINTS", "monas:abc:106.8")
	_, err := New(context.Background(), WithoutDotEnv())
	if err == nil || !strings.Contains(err.Error(), "GEOFENCE_POINTS") {
		t.Fatalf("err = %v, want GEOFENCE_POINTS error", err)
	}
}

func TestNewWorkerRequiresRabbitMQ(t *testing.T) {
	t.Setenv("DB_CONNECTION", "postgres://fleet:fleet@localhost:1/fleet")
	t.Setenv("RABBITMQ_URI", "")
	_, err := NewWorker(context.Background(), WithoutDotEnv())
	if err == nil || !strings.Contains(err.Error(), "RABBITMQ_URI") {
		t.Fatalf("err = %v, want RABBITMQ_URI error", err)
	}
}

func TestNewPublisherValidation(t *testing.T) {
	t.Setenv("MQTT_BROKER_URI", "")
	if _, err := NewPublisher(context.Background(), WithoutDotEnv()); err == nil || !strings.Contains(err.Error(), "MQTT_BROKER_URI") {
		t.Fatalf("err = %v, want MQTT_BROKER_URI error", err)
	}

	t.Setenv("MQTT_BROKER_URI", "tcp://localhost:1")
	t.Setenv("PUBLISH_INTERVAL", "500ms")
	if _, err := NewPublisher(context.Background(), WithoutDotEnv()); err == nil || !strings.Contains(err.Error(), "PUBLISH_INTERVAL") {
		t.Fatalf("err = %v, want PUBLISH_INTERVAL error", err)
	}
}
