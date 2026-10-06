package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"time"

	"github.com/ekowdd89/test-teknis-backend/internal/fleet"
	"github.com/ekowdd89/test-teknis-backend/internal/postgres"
	"github.com/ekowdd89/test-teknis-backend/pkg/env"
	"github.com/ekowdd89/test-teknis-backend/pkg/mqtt"
	"github.com/ekowdd89/test-teknis-backend/pkg/rabbitmq"
)

// OptFunc berlaku untuk semua binary (server, worker, publisher).
type OptFunc func(*Common) error

func WithEnvPrefix(p string) OptFunc {
	return func(c *Common) (err error) {
		c.envPrefix = p
		return
	}
}

func WithoutDotEnv() OptFunc {
	return func(c *Common) (err error) {
		c.dotenv = false
		return
	}
}

// WithVersion diisi dari main.version (ldflags -X saat docker build).
func WithVersion(v string) OptFunc {
	return func(c *Common) (err error) {
		c.version = v
		return
	}
}

// Common berisi bagian yang sama untuk semua binary: konfigurasi umum,
// logger, dan daftar resource yang harus ditutup saat shutdown.
type Common struct {
	envPrefix string
	dotenv    bool
	version   string

	LogLevel string `env:"LOG_LEVEL" envDefault:"info" json:"log_level"`

	logger  *slog.Logger
	closers []func(context.Context) error
}

type PostgresConfig struct {
	DBConnection     string        `env:"DB_CONNECTION,required,notEmpty,expand" json:"db_connection"`
	DBConnectTimeout time.Duration `env:"DB_CONNECT_TIMEOUT" envDefault:"30s" json:"db_connect_timeout"`
}

type RabbitMQConfig struct {
	RabbitMQURI            string        `env:"RABBITMQ_URI,expand" json:"rabbitmq_uri"`
	RabbitMQExchange       string        `env:"RABBITMQ_EXCHANGE" envDefault:"fleet.events" json:"rabbitmq_exchange"`
	RabbitMQQueue          string        `env:"RABBITMQ_QUEUE" envDefault:"geofence_alerts" json:"rabbitmq_queue"`
	RabbitMQRoutingKey     string        `env:"RABBITMQ_ROUTING_KEY" envDefault:"geofence.entry" json:"rabbitmq_routing_key"`
	RabbitMQPrefetch       int           `env:"RABBITMQ_PREFETCH" envDefault:"10" json:"rabbitmq_prefetch"`
	RabbitMQConnectTimeout time.Duration `env:"RABBITMQ_CONNECT_TIMEOUT" envDefault:"30s" json:"rabbitmq_connect_timeout"`
}

type MQTTConfig struct {
	MQTTBrokerURI      string        `env:"MQTT_BROKER_URI,expand" json:"mqtt_broker_uri"`
	MQTTClientID       string        `env:"MQTT_CLIENT_ID" json:"mqtt_client_id"`
	MQTTUsername       string        `env:"MQTT_USERNAME" json:"mqtt_username"`
	MQTTPassword       string        `env:"MQTT_PASSWORD" json:"-"`
	MQTTConnectTimeout time.Duration `env:"MQTT_CONNECT_TIMEOUT" envDefault:"30s" json:"mqtt_connect_timeout"`
}

func newCommon() Common {
	return Common{dotenv: true, version: "dev"}
}

// setup menerapkan opsi, membaca env ke target (struct yang meng-embed
// Common), lalu menyiapkan logger.
func (c *Common) setup(target any, opts []OptFunc) (err error) {
	for _, opt := range opts {
		if err = opt(c); err != nil {
			return
		}
	}
	if err = env.Load(target, env.Options{Prefix: c.envPrefix, DotEnv: c.dotenv}); err != nil {
		return
	}
	return c.initLogger()
}

func (c *Common) Logger() *slog.Logger {
	return c.logger
}

func (c *Common) initLogger() (err error) {
	var level slog.Level
	if err = level.UnmarshalText([]byte(c.LogLevel)); err != nil {
		return fmt.Errorf("invalid LOG_LEVEL %q: %w", c.LogLevel, err)
	}
	c.logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(c.logger)
	return
}

func (c *Common) initPostgres(ctx context.Context, cfg PostgresConfig) (p *postgres.Postgres, err error) {
	p, err = postgres.New(
		postgres.WithDbUri(cfg.DBConnection),
		postgres.WithLogger(c.logger.With("logger-name", "postgres")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate postgres: %w", err)
	}
	c.closers = append(c.closers, func(context.Context) error { return p.Close() })

	// Postgres bisa belum siap saat container baru start; Connect mencoba ulang
	// sampai berhasil atau timeout.
	connectCtx, cancel := context.WithTimeout(ctx, cfg.DBConnectTimeout)
	defer cancel()
	if err = p.Connect(connectCtx); err != nil {
		return nil, fmt.Errorf("failed to connect postgres: %w", err)
	}
	return p, nil
}

// initRabbitMQ terhubung ke RabbitMQ dan mendeklarasikan topologi
// (exchange, queue, binding, DLQ). Reconnect otomatis setelahnya.
func (c *Common) initRabbitMQ(ctx context.Context, cfg RabbitMQConfig) (rb *rabbitmq.Rabbitmq, err error) {
	opts := append(
		fleet.RabbitTopology(cfg.RabbitMQExchange, cfg.RabbitMQQueue, cfg.RabbitMQRoutingKey),
		rabbitmq.WithURI(cfg.RabbitMQURI),
		rabbitmq.WithPrefetch(cfg.RabbitMQPrefetch),
		rabbitmq.WithLogger(c.logger.With("logger-name", "rabbitmq")),
	)
	rb, err = rabbitmq.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate rabbitmq: %w", err)
	}
	c.closers = append(c.closers, func(context.Context) error { return rb.Close() })

	connectCtx, cancel := context.WithTimeout(ctx, cfg.RabbitMQConnectTimeout)
	defer cancel()
	if err = rb.Connect(connectCtx); err != nil {
		return nil, fmt.Errorf("failed to connect rabbitmq: %w", err)
	}
	return rb, nil
}

// newMQTT membuat client MQTT tanpa langsung terhubung, agar subscription
// bisa didaftarkan dulu (lihat connectMQTT).
func (c *Common) newMQTT(cfg MQTTConfig, defaultClientID string, cleanSession bool) (m *mqtt.Mqtt, err error) {
	clientID := cfg.MQTTClientID
	if clientID == "" {
		clientID = defaultClientID
	}
	m, err = mqtt.New(
		mqtt.WithBrokerId(cfg.MQTTBrokerURI),
		mqtt.WithClientId(clientID),
		mqtt.WithCredential(cfg.MQTTUsername, cfg.MQTTPassword),
		mqtt.WithCleanSession(cleanSession),
		mqtt.WithLogger(c.logger.With("logger-name", "mqtt")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate mqtt: %w", err)
	}
	c.closers = append(c.closers, func(context.Context) error { return m.Close() })
	return m, nil
}

func (c *Common) connectMQTT(ctx context.Context, m *mqtt.Mqtt, cfg MQTTConfig) (err error) {
	connectCtx, cancel := context.WithTimeout(ctx, cfg.MQTTConnectTimeout)
	defer cancel()
	if err = m.Connect(connectCtx); err != nil {
		return fmt.Errorf("failed to connect mqtt: %w", err)
	}
	return nil
}

// close menutup resource dengan urutan terbalik dari pembuatannya.
func (c *Common) close(ctx context.Context, err error) error {
	for i := len(c.closers) - 1; i >= 0; i-- {
		err = errors.Join(err, c.closers[i](ctx))
	}
	c.closers = nil
	return err
}

func isCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, rabbitmq.ErrClosed)
}

// redactURL menyamarkan password di connection string. Bila bukan URL
// (format key=value), seluruh nilai disamarkan agar aman.
func redactURL(s string) string {
	u, err := url.Parse(s)
	if err != nil || u.Scheme == "" || u.Host == "" {
		if s == "" {
			return ""
		}
		return "xxxxx"
	}
	// url.Redacted tidak menyamarkan password di query (?password=...).
	if q := u.Query(); q.Has("password") {
		q.Set("password", "xxxxx")
		u.RawQuery = q.Encode()
	}
	return u.Redacted()
}
