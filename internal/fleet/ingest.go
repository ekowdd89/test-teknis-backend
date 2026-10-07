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
	"github.com/ekowdd89/test-teknis-backend/pkg/mqtt"
)

type IngestorOptFunc func(*Ingestor) error

// Ingestor memproses lokasi dari MQTT: simpan lokasi, cek geofence, dan tulis
// event ke outbox dalam SATU transaksi (transactional outbox).
type Ingestor struct {
	pg           *postgres.Postgres
	queries      *sqlc.Queries
	geofences    []Geofence
	radiusMeters float64
	exchange     string
	routingKey   string
	maxAttempts  int
	retryDelay   time.Duration
	logger       *slog.Logger
}

func WithIngestGeofences(gs []Geofence, radiusMeters float64) IngestorOptFunc {
	return func(i *Ingestor) error {
		if radiusMeters <= 0 {
			return errors.New("fleet: geofence radius must be > 0")
		}
		i.geofences = gs
		i.radiusMeters = radiusMeters
		return nil
	}
}

// WithIngestDestination menentukan exchange & routing key event di outbox.
func WithIngestDestination(exchange, routingKey string) IngestorOptFunc {
	return func(i *Ingestor) error {
		if exchange == "" || routingKey == "" {
			return errors.New("fleet: exchange and routing key are required")
		}
		i.exchange, i.routingKey = exchange, routingKey
		return nil
	}
}

func WithIngestLogger(l *slog.Logger) IngestorOptFunc {
	return func(i *Ingestor) error {
		i.logger = l
		return nil
	}
}

func NewIngestor(pg *postgres.Postgres, opts ...IngestorOptFunc) (i *Ingestor, err error) {
	if pg == nil {
		return nil, errors.New("fleet: postgres is required")
	}
	i = &Ingestor{
		pg:           pg,
		queries:      sqlc.New(pg.Db()),
		radiusMeters: 50,
		exchange:     "fleet.events",
		routingKey:   "geofence.entry",
		maxAttempts:  3,
		retryDelay:   time.Second,
		logger:       slog.Default(),
	}
	for _, opt := range opts {
		if err = opt(i); err != nil {
			return nil, err
		}
	}
	return i, nil
}

// MQTTHandler mengubah Ingestor menjadi handler pkg/mqtt. ctx dipakai untuk
// membatalkan proses saat shutdown.
//
// MQTT tidak punya nack: pesan di-ack setelah handler selesai. Karena itu
// error sementara (mis. database down) dicoba ulang beberapa kali di sini,
// sedangkan pesan tidak valid langsung dibuang dengan log.
func (i *Ingestor) MQTTHandler(ctx context.Context) mqtt.Message {
	return func(topic string, payload []byte) {
		for attempt := 1; ; attempt++ {
			err := i.Handle(ctx, topic, payload)
			if err == nil {
				return
			}
			if errors.Is(err, ErrInvalidMessage) {
				i.logger.Warn("mqtt message dropped", "topic", topic, "error", err)
				return
			}
			if attempt >= i.maxAttempts || ctx.Err() != nil {
				i.logger.Error("mqtt message failed", "topic", topic, "attempt", attempt, "error", err)
				return
			}
			i.logger.Warn("mqtt message failed, retrying", "topic", topic, "attempt", attempt, "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(i.retryDelay):
			}
		}
	}
}

func (i *Ingestor) Handle(ctx context.Context, topic string, payload []byte) (err error) {
	loc, err := ParseLocation(topic, payload)
	if err != nil {
		return err
	}

	var entered []string
	err = i.pg.WithTx(ctx, func(tx *sql.Tx) error {
		q := i.queries.WithTx(tx)

		n, err := q.InsertVehicleLocation(ctx, sqlc.InsertVehicleLocationParams{
			VehicleID: loc.VehicleID,
			Latitude:  loc.Latitude,
			Longitude: loc.Longitude,
			Timestamp: loc.Timestamp,
		})
		if err != nil {
			return fmt.Errorf("insert location: %w", err)
		}
		if n == 0 {
			// Duplikat (QoS 1 bisa mengirim ulang): geofence sudah diproses sebelumnya.
			i.logger.Info("duplicate location ignored (vehicle_id + timestamp already stored)",
				"vehicle_id", loc.VehicleID, "timestamp", loc.Timestamp,
				"latitude", loc.Latitude, "longitude", loc.Longitude)
			return nil
		}

		for _, g := range i.geofences {
			inside := DistanceMeters(loc.Latitude, loc.Longitude, g.Latitude, g.Longitude) <= i.radiusMeters
			if !inside {
				if _, err = q.ExitGeofence(ctx, sqlc.ExitGeofenceParams{VehicleID: loc.VehicleID, GeofenceID: g.ID}); err != nil {
					return fmt.Errorf("exit geofence %s: %w", g.ID, err)
				}
				continue
			}

			// Event hanya saat baru masuk, bukan selama kendaraan masih di dalam.
			n, err := q.EnterGeofence(ctx, sqlc.EnterGeofenceParams{VehicleID: loc.VehicleID, GeofenceID: g.ID, EnteredTs: loc.Timestamp})
			if err != nil {
				return fmt.Errorf("enter geofence %s: %w", g.ID, err)
			}
			if n == 0 {
				continue
			}
			if err = i.writeOutbox(ctx, q, loc, g); err != nil {
				return err
			}
			entered = append(entered, g.ID)
		}
		return nil
	})
	if err != nil {
		return err
	}

	for _, id := range entered {
		i.logger.Info("geofence entry detected", "vehicle_id", loc.VehicleID, "geofence_id", id, "timestamp", loc.Timestamp)
	}
	return nil
}

func (i *Ingestor) writeOutbox(ctx context.Context, q *sqlc.Queries, loc VehicleLocation, g Geofence) (err error) {
	payload, err := json.Marshal(GeofenceAlertMessage{
		VehicleID: loc.VehicleID,
		Event:     EventGeofenceEntry,
		Location:  Location{Latitude: loc.Latitude, Longitude: loc.Longitude},
		Timestamp: loc.Timestamp,
	})
	if err != nil {
		return fmt.Errorf("encode payload: %w", err)
	}
	headers, err := json.Marshal(map[string]string{"geofence_id": g.ID})
	if err != nil {
		return fmt.Errorf("encode headers: %w", err)
	}
	_, err = q.InsertOutboxEvent(ctx, sqlc.InsertOutboxEventParams{
		AggregateType: "vehicle",
		AggregateID:   loc.VehicleID,
		EventType:     EventGeofenceEntry,
		Exchange:      i.exchange,
		RoutingKey:    i.routingKey,
		Headers:       headers,
		Payload:       payload,
		DedupKey:      dedupKey(loc.VehicleID, g.ID, loc.Timestamp),
	})
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}
