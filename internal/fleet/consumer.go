package fleet

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/ekowdd89/test-teknis-backend/internal/postgres"
	"github.com/ekowdd89/test-teknis-backend/internal/postgres/sqlc"
	"github.com/ekowdd89/test-teknis-backend/pkg/rabbitmq"
	"github.com/jackc/pgx/v5/pgconn"
)

const DefaultConsumerName = "geofence-alert-worker"

// Consumer memproses pesan geofence dari RabbitMQ secara idempotent:
// insert inbox + simpan event dalam satu transaksi, baru ack.
type Consumer struct {
	pg      *postgres.Postgres
	queries *sqlc.Queries
	name    string
	logger  *slog.Logger
}

func NewConsumer(pg *postgres.Postgres, name string, logger *slog.Logger) (c *Consumer, err error) {
	if pg == nil {
		return nil, errors.New("fleet: postgres is required")
	}
	if name == "" {
		name = DefaultConsumerName
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Consumer{pg: pg, queries: sqlc.New(pg.Db()), name: name, logger: logger}, nil
}

// ParseAlert memvalidasi pesan RabbitMQ. Error yang dikembalikan bersifat
// permanen (pesan tidak akan valid walau dicoba ulang).
func ParseAlert(msg rabbitmq.Message) (alert GeofenceAlertMessage, geofenceID string, err error) {
	if msg.MessageId == "" {
		return alert, "", fmt.Errorf("%w: missing message_id", ErrInvalidMessage)
	}
	if err = json.Unmarshal(msg.Body, &alert); err != nil {
		return alert, "", fmt.Errorf("%w: decode body: %v", ErrInvalidMessage, err)
	}
	geofenceID, _ = msg.Headers["geofence_id"].(string)
	switch {
	case geofenceID == "":
		return alert, "", fmt.Errorf("%w: missing geofence_id header", ErrInvalidMessage)
	case !vehicleIDPattern.MatchString(alert.VehicleID):
		return alert, "", fmt.Errorf("%w: invalid vehicle_id", ErrInvalidMessage)
	case alert.Event == "":
		return alert, "", fmt.Errorf("%w: missing event", ErrInvalidMessage)
	}
	return alert, geofenceID, nil
}

// Handle implements rabbitmq.Handler.
func (c *Consumer) Handle(ctx context.Context, msg rabbitmq.Message) (err error) {
	alert, geofenceID, err := ParseAlert(msg)
	if err != nil {
		return rabbitmq.Permanent(err)
	}

	duplicate := false
	err = c.pg.WithTx(ctx, func(tx *sql.Tx) error {
		q := c.queries.WithTx(tx)
		n, err := q.InsertInboxMessage(ctx, sqlc.InsertInboxMessageParams{
			Consumer:  c.name,
			MessageID: msg.MessageId,
			EventType: alert.Event,
		})
		if err != nil {
			return fmt.Errorf("insert inbox: %w", err)
		}
		if n == 0 {
			duplicate = true
			return nil
		}
		_, err = q.InsertGeofenceEvent(ctx, sqlc.InsertGeofenceEventParams{
			MessageID:  msg.MessageId,
			VehicleID:  alert.VehicleID,
			GeofenceID: geofenceID,
			EventType:  alert.Event,
			Latitude:   alert.Location.Latitude,
			Longitude:  alert.Location.Longitude,
			Timestamp:  alert.Timestamp,
		})
		if err != nil {
			return fmt.Errorf("insert geofence event: %w", err)
		}
		return nil
	})
	if err != nil {
		if isPermanentDBError(err) {
			// Data ditolak database (mis. nilai terlalu panjang): mengulang tidak
			// akan pernah berhasil, jadi kirim ke DLQ alih-alih requeue tanpa akhir.
			return rabbitmq.Permanent(err)
		}
		return err // sementara (mis. database down): di-requeue oleh pkg/rabbitmq
	}

	if duplicate {
		c.logger.Info("duplicate message skipped", "message_id", msg.MessageId)
		return nil
	}
	c.logger.Info("geofence alert received",
		"message_id", msg.MessageId,
		"vehicle_id", alert.VehicleID,
		"geofence_id", geofenceID,
		"event", alert.Event,
		"latitude", alert.Location.Latitude,
		"longitude", alert.Location.Longitude,
		"timestamp", alert.Timestamp,
	)
	return nil
}

// isPermanentDBError: SQLSTATE kelas 22 (data exception) dan 23 (integrity
// constraint violation) selalu gagal untuk data yang sama. Error koneksi atau
// timeout bukan *pgconn.PgError sehingga tetap dianggap sementara.
func isPermanentDBError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || len(pgErr.Code) < 2 {
		return false
	}
	switch pgErr.Code[:2] {
	case "22", "23":
		return true
	}
	return false
}
