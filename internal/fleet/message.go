package fleet

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const EventGeofenceEntry = "geofence_entry"

// ErrInvalidMessage menandai pesan yang tidak akan pernah valid walau dicoba ulang.
var ErrInvalidMessage = errors.New("invalid message")

var vehicleIDPattern = regexp.MustCompile(`^[A-Za-z0-9-]{1,20}$`)

// VehicleLocation adalah payload MQTT di topik /fleet/vehicle/{vehicle_id}/location.
type VehicleLocation struct {
	VehicleID string  `json:"vehicle_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Timestamp int64   `json:"timestamp"`
}

type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// GeofenceAlertMessage adalah body pesan RabbitMQ (schema GeofenceAlertMessage
// di api/openapi-spec.yaml). geofence_id dikirim lewat header AMQP.
type GeofenceAlertMessage struct {
	VehicleID string   `json:"vehicle_id"`
	Event     string   `json:"event"`
	Location  Location `json:"location"`
	Timestamp int64    `json:"timestamp"`
}

// ParseLocation memvalidasi topik dan payload MQTT.
func ParseLocation(topic string, payload []byte) (loc VehicleLocation, err error) {
	parts := strings.Split(strings.Trim(topic, "/"), "/")
	if len(parts) != 4 || parts[0] != "fleet" || parts[1] != "vehicle" || parts[3] != "location" {
		return loc, fmt.Errorf("%w: unexpected topic %q", ErrInvalidMessage, topic)
	}
	topicVehicleID := parts[2]

	// Pointer agar field yang hilang bisa dibedakan dari nilai 0.
	var raw struct {
		VehicleID *string  `json:"vehicle_id"`
		Latitude  *float64 `json:"latitude"`
		Longitude *float64 `json:"longitude"`
		Timestamp *int64   `json:"timestamp"`
	}
	if err = json.Unmarshal(payload, &raw); err != nil {
		return loc, fmt.Errorf("%w: decode payload: %v", ErrInvalidMessage, err)
	}
	if raw.VehicleID == nil || raw.Latitude == nil || raw.Longitude == nil || raw.Timestamp == nil {
		return loc, fmt.Errorf("%w: vehicle_id, latitude, longitude and timestamp are required", ErrInvalidMessage)
	}
	loc = VehicleLocation{VehicleID: *raw.VehicleID, Latitude: *raw.Latitude, Longitude: *raw.Longitude, Timestamp: *raw.Timestamp}

	switch {
	case !vehicleIDPattern.MatchString(loc.VehicleID):
		return loc, fmt.Errorf("%w: vehicle_id must be 1-20 characters of [A-Za-z0-9-]", ErrInvalidMessage)
	case loc.VehicleID != topicVehicleID:
		return loc, fmt.Errorf("%w: vehicle_id %q does not match topic %q", ErrInvalidMessage, loc.VehicleID, topicVehicleID)
	case loc.Latitude < -90 || loc.Latitude > 90:
		return loc, fmt.Errorf("%w: latitude out of range", ErrInvalidMessage)
	case loc.Longitude < -180 || loc.Longitude > 180:
		return loc, fmt.Errorf("%w: longitude out of range", ErrInvalidMessage)
	case loc.Timestamp <= 0:
		return loc, fmt.Errorf("%w: timestamp must be > 0", ErrInvalidMessage)
	}
	return loc, nil
}

// LocationTopic membangun topik MQTT untuk kendaraan tertentu.
func LocationTopic(vehicleID string) string {
	return "/fleet/vehicle/" + vehicleID + "/location"
}

func dedupKey(vehicleID, geofenceID string, ts int64) string {
	return fmt.Sprintf("%s:%s:%s:%d", EventGeofenceEntry, vehicleID, geofenceID, ts)
}
