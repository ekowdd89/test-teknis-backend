package fleet

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ekowdd89/test-teknis-backend/internal/postgres"
	"github.com/ekowdd89/test-teknis-backend/internal/postgres/sqlc"
	"github.com/ekowdd89/test-teknis-backend/pkg/rabbitmq"
)

// Publisher adalah bagian pkg/rabbitmq yang dipakai relay.
type Publisher interface {
	PublishMessage(ctx context.Context, exchange, key string, p rabbitmq.Publishing) error
	IsConnected() bool
}

type RelayOptFunc func(*Relay) error

// Relay memindahkan event dari tabel outbox_events ke RabbitMQ.
// Beberapa instance aman berjalan paralel (FOR UPDATE SKIP LOCKED).
type Relay struct {
	pg          *postgres.Postgres
	queries     *sqlc.Queries
	publisher   Publisher
	interval    time.Duration
	batchSize   int32
	maxAttempts int32
	maxBackoff  time.Duration
	// statsInterval: seberapa sering jumlah event per status dicatat ke log.
	statsInterval time.Duration
	logger        *slog.Logger
}

func WithRelayInterval(d time.Duration) RelayOptFunc {
	return func(r *Relay) error {
		if d <= 0 {
			return errors.New("fleet: relay interval must be > 0")
		}
		r.interval = d
		return nil
	}
}

func WithRelayBatchSize(n int32) RelayOptFunc {
	return func(r *Relay) error {
		if n <= 0 {
			return errors.New("fleet: relay batch size must be > 0")
		}
		r.batchSize = n
		return nil
	}
}

func WithRelayMaxAttempts(n int32) RelayOptFunc {
	return func(r *Relay) error {
		if n <= 0 {
			return errors.New("fleet: relay max attempts must be > 0")
		}
		r.maxAttempts = n
		return nil
	}
}

// WithRelayStatsInterval mengatur seberapa sering jumlah event per status
// (pending/published/failed) dicatat ke log.
func WithRelayStatsInterval(d time.Duration) RelayOptFunc {
	return func(r *Relay) error {
		if d <= 0 {
			return errors.New("fleet: relay stats interval must be > 0")
		}
		r.statsInterval = d
		return nil
	}
}

func WithRelayLogger(l *slog.Logger) RelayOptFunc {
	return func(r *Relay) error {
		r.logger = l
		return nil
	}
}

func NewRelay(pg *postgres.Postgres, publisher Publisher, opts ...RelayOptFunc) (r *Relay, err error) {
	if pg == nil || publisher == nil {
		return nil, errors.New("fleet: postgres and publisher are required")
	}
	r = &Relay{
		pg:            pg,
		queries:       sqlc.New(pg.Db()),
		publisher:     publisher,
		interval:      time.Second,
		batchSize:     100,
		maxAttempts:   10,
		maxBackoff:    5 * time.Minute,
		statsInterval: time.Minute,
		logger:        slog.Default(),
	}
	for _, opt := range opts {
		if err = opt(r); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Run berjalan sampai ctx selesai.
func (r *Relay) Run(ctx context.Context) error {
	r.logger.Info("outbox relay started", "interval", r.interval.String(), "batch_size", r.batchSize)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	stats := time.NewTicker(r.statsInterval)
	defer stats.Stop()
	r.logStats(ctx)
	for {
		r.drain(ctx)
		select {
		case <-ctx.Done():
			r.logger.Info("outbox relay stopped")
			return nil
		case <-stats.C:
			r.logStats(ctx)
		case <-ticker.C:
		}
	}
}

// logStats mencatat jumlah event per status. Event "failed" tidak pernah
// dicoba ulang otomatis, jadi dilaporkan sebagai WARN sampai di-requeue
// (make outbox-requeue) atau dihapus.
func (r *Relay) logStats(ctx context.Context) {
	rows, err := r.queries.CountOutboxEventsByStatus(ctx)
	if err != nil {
		if ctx.Err() == nil {
			r.logger.Warn("outbox stats failed", "error", err)
		}
		return
	}
	counts := map[string]int64{"pending": 0, "published": 0, "failed": 0}
	for _, row := range rows {
		counts[row.Status] = row.Total
	}
	attrs := []any{"pending", counts["pending"], "published", counts["published"], "failed", counts["failed"]}
	if counts["failed"] > 0 {
		r.logger.Warn("outbox has failed events (run: make outbox-requeue)", attrs...)
		return
	}
	r.logger.Info("outbox stats", attrs...)
}

// drain mengirim batch berulang sampai outbox kosong atau terjadi error.
func (r *Relay) drain(ctx context.Context) {
	for ctx.Err() == nil {
		n, err := r.RelayBatch(ctx)
		if err != nil {
			if ctx.Err() == nil {
				r.logger.Error("outbox relay batch failed", "error", err)
			}
			return
		}
		if n < int(r.batchSize) {
			return
		}
	}
}

// RelayBatch mengirim satu batch dan mengembalikan jumlah event yang terkirim.
func (r *Relay) RelayBatch(ctx context.Context) (published int, err error) {
	// RabbitMQ down: jangan sentuh outbox, biarkan event tetap pending
	// (tidak menghabiskan jatah attempts).
	if !r.publisher.IsConnected() {
		return 0, nil
	}

	err = r.pg.WithTx(ctx, func(tx *sql.Tx) error {
		q := r.queries.WithTx(tx)
		events, err := q.ClaimPendingOutboxEvents(ctx, r.batchSize)
		if err != nil {
			return fmt.Errorf("claim outbox: %w", err)
		}
		for _, e := range events {
			if errPub := r.publish(ctx, e); errPub != nil {
				if e.Attempts+1 >= r.maxAttempts {
					r.logger.Error("outbox event failed permanently, will not be retried (run: make outbox-requeue)",
						"id", e.ID, "event_type", e.EventType, "aggregate_id", e.AggregateID, "attempts", e.Attempts+1, "error", errPub)
				} else {
					r.logger.Warn("outbox publish failed, will retry", "id", e.ID, "attempts", e.Attempts+1,
						"retry_in_seconds", r.backoffSeconds(e.Attempts), "error", errPub)
				}
				if err = q.MarkOutboxEventFailed(ctx, sqlc.MarkOutboxEventFailedParams{
					ID:             e.ID,
					LastError:      errPub.Error(),
					MaxAttempts:    r.maxAttempts,
					BackoffSeconds: r.backoffSeconds(e.Attempts),
				}); err != nil {
					return fmt.Errorf("mark outbox failed: %w", err)
				}
				// Kemungkinan besar broker bermasalah: hentikan batch, sisa event
				// tetap pending dan dicoba di tick berikutnya.
				return nil
			}
			if err = q.MarkOutboxEventPublished(ctx, e.ID); err != nil {
				return fmt.Errorf("mark outbox published: %w", err)
			}
			published++
			r.logger.Debug("outbox event published", "id", e.ID, "event_type", e.EventType, "aggregate_id", e.AggregateID)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if published > 0 {
		r.logger.Info("outbox events published", "count", published)
	}
	return published, nil
}

func (r *Relay) publish(ctx context.Context, e sqlc.ClaimPendingOutboxEventsRow) (err error) {
	var headers map[string]any
	if len(e.Headers) > 0 {
		if err = json.Unmarshal(e.Headers, &headers); err != nil {
			return fmt.Errorf("decode headers: %w", err)
		}
	}
	return r.publisher.PublishMessage(ctx, e.Exchange, e.RoutingKey, rabbitmq.Publishing{
		ContentType: "application/json",
		MessageId:   e.ID.String(),
		Type:        e.EventType,
		Headers:     headers,
		Body:        e.Payload,
	})
}

// backoffSeconds: 1, 2, 4, ... detik, dibatasi maxBackoff.
func (r *Relay) backoffSeconds(attempts int32) int32 {
	maxSec := int32(r.maxBackoff / time.Second)
	if attempts >= 30 {
		return maxSec
	}
	return min(int32(1)<<attempts, maxSec)
}
