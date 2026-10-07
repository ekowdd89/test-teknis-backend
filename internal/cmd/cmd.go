package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/ekowdd89/test-teknis-backend/internal/fleet"
	"github.com/ekowdd89/test-teknis-backend/internal/httpserver"
	"github.com/ekowdd89/test-teknis-backend/internal/postgres"
	"github.com/ekowdd89/test-teknis-backend/internal/postgres/sqlc"
	"github.com/ekowdd89/test-teknis-backend/pkg/mqtt"
	"github.com/ekowdd89/test-teknis-backend/pkg/rabbitmq"
	"golang.org/x/sync/errgroup"
)

var _ slog.LogValuer = CMD{}

// CMD adalah composition root binary server:
// REST API + MQTT ingest + outbox relay ke RabbitMQ.
type CMD struct {
	Common
	PostgresConfig
	RabbitMQConfig
	MQTTConfig

	HTTPAddr        string        `env:"HTTP_ADDR,expand" envDefault:":8080" json:"http_addr"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"10s" json:"shutdown_timeout"`

	MQTTTopic            string  `env:"MQTT_TOPIC" envDefault:"/fleet/vehicle/+/location" json:"mqtt_topic"`
	GeofencePoints       string  `env:"GEOFENCE_POINTS" envDefault:"bundaran-hi:-6.1950:106.8230,monas:-6.1754:106.8272,blok-m:-6.2443:106.8000" json:"geofence_points"`
	GeofenceRadiusMeters float64 `env:"GEOFENCE_RADIUS_METERS" envDefault:"50" json:"geofence_radius_meters"`

	OutboxPollInterval  time.Duration `env:"OUTBOX_POLL_INTERVAL" envDefault:"1s" json:"outbox_poll_interval"`
	OutboxBatchSize     int32         `env:"OUTBOX_BATCH_SIZE" envDefault:"100" json:"outbox_batch_size"`
	OutboxMaxAttempts   int32         `env:"OUTBOX_MAX_ATTEMPTS" envDefault:"10" json:"outbox_max_attempts"`
	OutboxStatsInterval time.Duration `env:"OUTBOX_STATS_INTERVAL" envDefault:"1m" json:"outbox_stats_interval"`

	geofences []fleet.Geofence

	h     *httpserver.HTTPServer
	p     *postgres.Postgres
	rb    *rabbitmq.Rabbitmq
	m     *mqtt.Mqtt
	relay *fleet.Relay
}

// New menyiapkan semua dependensi. ctx dipakai untuk inisialisasi dan juga
// sebagai umur handler MQTT, jadi berikan ctx yang sama dengan Run
// (mis. ctx dari signal.NotifyContext).
func New(ctx context.Context, opts ...OptFunc) (c *CMD, err error) {
	c = &CMD{Common: newCommon()}
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
	if c.geofences, err = fleet.ParseGeofences(c.GeofencePoints); err != nil {
		return nil, fmt.Errorf("invalid GEOFENCE_POINTS: %w", err)
	}
	if c.p, err = c.initPostgres(ctx, c.PostgresConfig); err != nil {
		return
	}
	if err = c.initRelay(ctx); err != nil {
		return
	}
	if err = c.initIngest(ctx); err != nil {
		return
	}
	if err = c.initHTTPServer(); err != nil {
		return
	}
	return
}

// LogValue menampilkan konfigurasi tanpa membocorkan password.
func (c CMD) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("version", c.version),
		slog.String("http_addr", c.HTTPAddr),
		slog.String("db_connection", redactURL(c.DBConnection)),
		slog.String("mqtt_broker_uri", redactURL(c.MQTTBrokerURI)),
		slog.String("mqtt_topic", c.MQTTTopic),
		slog.String("rabbitmq_uri", redactURL(c.RabbitMQURI)),
		slog.String("rabbitmq_exchange", c.RabbitMQExchange),
		slog.String("rabbitmq_routing_key", c.RabbitMQRoutingKey),
		slog.Int("geofences", len(c.geofences)),
		slog.Float64("geofence_radius_meters", c.GeofenceRadiusMeters),
		slog.String("log_level", c.LogLevel),
	)
}

// initRelay: tanpa RABBITMQ_URI event tetap ditulis ke outbox (status pending)
// dan akan terkirim setelah RabbitMQ dikonfigurasi.
func (c *CMD) initRelay(ctx context.Context) (err error) {
	if c.RabbitMQURI == "" {
		c.logger.Warn("RABBITMQ_URI is empty, outbox relay disabled")
		return nil
	}
	if c.rb, err = c.initRabbitMQ(ctx, c.RabbitMQConfig); err != nil {
		return
	}
	c.relay, err = fleet.NewRelay(c.p, c.rb,
		fleet.WithRelayInterval(c.OutboxPollInterval),
		fleet.WithRelayBatchSize(c.OutboxBatchSize),
		fleet.WithRelayMaxAttempts(c.OutboxMaxAttempts),
		fleet.WithRelayStatsInterval(c.OutboxStatsInterval),
		fleet.WithRelayLogger(c.logger.With("logger-name", "outbox-relay")),
	)
	if err != nil {
		return fmt.Errorf("failed to instantiate outbox relay: %w", err)
	}
	return nil
}

func (c *CMD) initIngest(ctx context.Context) (err error) {
	if c.MQTTBrokerURI == "" {
		c.logger.Warn("MQTT_BROKER_URI is empty, mqtt ingest disabled")
		return nil
	}
	ingestor, err := fleet.NewIngestor(c.p,
		fleet.WithIngestGeofences(c.geofences, c.GeofenceRadiusMeters),
		fleet.WithIngestDestination(c.RabbitMQExchange, c.RabbitMQRoutingKey),
		fleet.WithIngestLogger(c.logger.With("logger-name", "mqtt-ingest")),
	)
	if err != nil {
		return fmt.Errorf("failed to instantiate ingestor: %w", err)
	}

	// Clean session false: broker menyimpan subscription & pesan QoS 1 saat
	// server restart, jadi lokasi yang dikirim selama downtime tidak hilang.
	if c.m, err = c.newMQTT(c.MQTTConfig, "fleet-server", false); err != nil {
		return
	}
	// Subscribe sebelum Connect agar handler sudah terdaftar ketika broker
	// mengirim pesan yang tertahan di sesi persisten.
	if err = c.m.Subscribe(ctx, c.MQTTTopic, 1, ingestor.MQTTHandler(ctx)); err != nil {
		return fmt.Errorf("failed to subscribe %s: %w", c.MQTTTopic, err)
	}
	return c.connectMQTT(ctx, c.m, c.MQTTConfig)
}

func (c *CMD) initHTTPServer() (err error) {
	l, err := net.Listen("tcp", c.HTTPAddr)
	if err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}
	// Bila New gagal, listener harus ditutup sendiri.
	defer func() {
		if err != nil {
			l.Close()
		}
	}()

	opts := []httpserver.OptFunc{
		httpserver.WithListener(l),
		httpserver.WithQuerier(sqlc.New(c.p.Db())),
		httpserver.WithHealthCheck("postgres", c.p.Ping),
		httpserver.WithShutdownTimeout(c.ShutdownTimeout),
		httpserver.WithLogger(c.logger.With("logger-name", "http-server")),
	}
	if c.m != nil {
		opts = append(opts, httpserver.WithHealthCheck("mqtt", connectedCheck(c.m.IsConnected, mqtt.ErrNotConnected)))
	}
	if c.rb != nil {
		opts = append(opts, httpserver.WithHealthCheck("rabbitmq", connectedCheck(c.rb.IsConnected, rabbitmq.ErrNotConnected)))
	}
	if c.h, err = httpserver.New(opts...); err != nil {
		return fmt.Errorf("failed to instantiate http server: %w", err)
	}
	return
}

// Run berjalan sampai ctx dibatalkan (mis. SIGINT/SIGTERM), lalu
// melakukan graceful shutdown dan menutup semua resource.
func (c *CMD) Run(ctx context.Context) (err error) {
	defer func() { err = c.close(context.Background(), err) }()

	c.logger.InfoContext(ctx, "server starting", "server", c)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return c.h.Serve(gctx) })
	if c.relay != nil {
		g.Go(func() error { return c.relay.Run(gctx) })
	}
	return g.Wait()
}

func connectedCheck(isConnected func() bool, errDown error) httpserver.HealthCheck {
	return func(context.Context) error {
		if !isConnected() {
			return errDown
		}
		return nil
	}
}
