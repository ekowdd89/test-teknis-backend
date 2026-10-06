package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ekowdd89/test-teknis-backend/internal/fleet"
	"github.com/ekowdd89/test-teknis-backend/pkg/mqtt"
)

var _ slog.LogValuer = PublisherCMD{}

// PublisherCMD adalah composition root mock publisher (dev/demo):
// mengirim lokasi kendaraan palsu ke MQTT secara berkala.
type PublisherCMD struct {
	Common
	MQTTConfig

	VehicleIDs      []string      `env:"VEHICLE_IDS" envDefault:"B1234XYZ,B5678ABC,B9012DEF" json:"vehicle_ids"`
	PublishInterval time.Duration `env:"PUBLISH_INTERVAL" envDefault:"2s" json:"publish_interval"`
	StepsPerLeg     int           `env:"PUBLISH_STEPS_PER_LEG" envDefault:"30" json:"steps_per_leg"`
	GeofencePoints  string        `env:"GEOFENCE_POINTS" envDefault:"bundaran-hi:-6.1950:106.8230,monas:-6.1754:106.8272,blok-m:-6.2443:106.8000" json:"geofence_points"`

	m   *mqtt.Mqtt
	sim *fleet.Simulator
}

func NewPublisher(ctx context.Context, opts ...OptFunc) (c *PublisherCMD, err error) {
	c = &PublisherCMD{Common: newCommon()}
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
	if c.MQTTBrokerURI == "" {
		return nil, errors.New("MQTT_BROKER_URI is required for publisher")
	}
	if c.PublishInterval < time.Second {
		// timestamp berresolusi detik: interval < 1 detik menghasilkan duplikat.
		return nil, errors.New("PUBLISH_INTERVAL must be >= 1s")
	}
	geofences, err := fleet.ParseGeofences(c.GeofencePoints)
	if err != nil {
		return nil, fmt.Errorf("invalid GEOFENCE_POINTS: %w", err)
	}
	for i := range c.VehicleIDs {
		c.VehicleIDs[i] = strings.TrimSpace(c.VehicleIDs[i])
	}
	if c.sim, err = fleet.NewSimulator(c.VehicleIDs, geofences, c.StepsPerLeg); err != nil {
		return
	}
	if c.m, err = c.newMQTT(c.MQTTConfig, "fleet-publisher", true); err != nil {
		return
	}
	if err = c.connectMQTT(ctx, c.m, c.MQTTConfig); err != nil {
		return
	}
	return
}

func (c PublisherCMD) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("version", c.version),
		slog.String("mqtt_broker_uri", redactURL(c.MQTTBrokerURI)),
		slog.Any("vehicle_ids", c.VehicleIDs),
		slog.String("publish_interval", c.PublishInterval.String()),
		slog.Int("steps_per_leg", c.StepsPerLeg),
		slog.String("log_level", c.LogLevel),
	)
}

func (c *PublisherCMD) Run(ctx context.Context) (err error) {
	defer func() { err = c.close(context.Background(), err) }()

	c.logger.InfoContext(ctx, "publisher starting", "publisher", c)
	ticker := time.NewTicker(c.PublishInterval)
	defer ticker.Stop()
	for {
		for _, loc := range c.sim.Next(time.Now()) {
			// QoS 1: at-least-once; duplikat ditangani server lewat UNIQUE (vehicle_id, timestamp).
			if err := c.m.PublishJSON(ctx, fleet.LocationTopic(loc.VehicleID), 1, false, loc); err != nil {
				c.logger.Warn("publish failed", "vehicle_id", loc.VehicleID, "error", err)
				continue
			}
			c.logger.Debug("location published", "vehicle_id", loc.VehicleID, "latitude", loc.Latitude, "longitude", loc.Longitude, "timestamp", loc.Timestamp)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
