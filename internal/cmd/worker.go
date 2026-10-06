package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/ekowdd89/test-teknis-backend/internal/fleet"
	"github.com/ekowdd89/test-teknis-backend/internal/postgres"
	"github.com/ekowdd89/test-teknis-backend/pkg/rabbitmq"
)

var _ slog.LogValuer = WorkerCMD{}

// WorkerCMD adalah composition root binary worker: consume queue
// geofence_alerts dan simpan ke geofence_events secara idempotent.
type WorkerCMD struct {
	Common
	PostgresConfig
	RabbitMQConfig

	ConsumerName string `env:"WORKER_CONSUMER_NAME" envDefault:"geofence-alert-worker" json:"consumer_name"`

	p        *postgres.Postgres
	rb       *rabbitmq.Rabbitmq
	consumer *fleet.Consumer
}

func NewWorker(ctx context.Context, opts ...OptFunc) (c *WorkerCMD, err error) {
	c = &WorkerCMD{Common: newCommon()}
	// Salinan pointer: "return nil, err" di bawah mengosongkan c sebelum defer berjalan.
	self := c
	defer func() {
		if err != nil {
			err = self.close(context.Background(), err)
			c = nil
		}
	}()

	if err = c.setup(c, opts); err != nil {
		return
	}
	if c.RabbitMQURI == "" {
		return nil, errors.New("RABBITMQ_URI is required for worker")
	}
	if c.p, err = c.initPostgres(ctx, c.PostgresConfig); err != nil {
		return
	}
	if c.rb, err = c.initRabbitMQ(ctx, c.RabbitMQConfig); err != nil {
		return
	}
	c.consumer, err = fleet.NewConsumer(c.p, c.ConsumerName, c.logger.With("logger-name", "consumer"))
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate consumer: %w", err)
	}
	return
}

func (c WorkerCMD) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("version", c.version),
		slog.String("db_connection", redactURL(c.DBConnection)),
		slog.String("rabbitmq_uri", redactURL(c.RabbitMQURI)),
		slog.String("rabbitmq_queue", c.RabbitMQQueue),
		slog.Int("rabbitmq_prefetch", c.RabbitMQPrefetch),
		slog.String("consumer_name", c.ConsumerName),
		slog.String("log_level", c.LogLevel),
	)
}

// Run consume sampai ctx dibatalkan; pesan yang sedang diproses
// diselesaikan dulu sebelum koneksi ditutup.
func (c *WorkerCMD) Run(ctx context.Context) (err error) {
	defer func() { err = c.close(context.Background(), err) }()

	c.logger.InfoContext(ctx, "worker starting", "worker", c)
	err = c.rb.Consume(ctx, c.RabbitMQQueue, c.consumer.Handle)
	if isCanceled(err) {
		return nil
	}
	return err
}
