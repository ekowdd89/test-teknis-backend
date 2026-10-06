package fleet

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/ekowdd89/test-teknis-backend/pkg/rabbitmq"
)

func TestParseGeofences(t *testing.T) {
	gs, err := ParseGeofences(" bundaran-hi:-6.1950:106.8230, monas:-6.1754:106.8272 ,")
	if err != nil {
		t.Fatalf("ParseGeofences: %v", err)
	}
	if len(gs) != 2 || gs[1] != (Geofence{ID: "monas", Latitude: -6.1754, Longitude: 106.8272}) {
		t.Fatalf("unexpected geofences: %+v", gs)
	}

	for _, in := range []string{"monas", "monas:-6.1:", ":1:2", "monas:91:0", "monas:0:181", "a:1:1,a:2:2"} {
		if _, err := ParseGeofences(in); err == nil {
			t.Errorf("ParseGeofences(%q): expected error", in)
		}
	}
}

func TestDistanceMeters(t *testing.T) {
	// Bundaran HI -> Monas kira-kira 2.2 km.
	d := DistanceMeters(-6.1950, 106.8230, -6.1754, 106.8272)
	if d < 2150 || d > 2300 {
		t.Fatalf("distance = %.0f m, want ~2200 m", d)
	}
	if d := DistanceMeters(-6.1754, 106.8272, -6.1754, 106.8272); d != 0 {
		t.Fatalf("same point distance = %f", d)
	}
	// 0.0004 derajat lintang ~ 44.5 m (di dalam radius 50 m).
	if d := DistanceMeters(-6.1754, 106.8272, -6.1758, 106.8272); math.Abs(d-44.5) > 0.5 {
		t.Fatalf("distance = %f, want ~44.5", d)
	}
}

func TestParseLocation(t *testing.T) {
	topic := "/fleet/vehicle/B1234XYZ/location"
	loc, err := ParseLocation(topic, []byte(`{"vehicle_id":"B1234XYZ","latitude":-6.2088,"longitude":106.8456,"timestamp":1715003456}`))
	if err != nil {
		t.Fatalf("ParseLocation: %v", err)
	}
	if loc != (VehicleLocation{VehicleID: "B1234XYZ", Latitude: -6.2088, Longitude: 106.8456, Timestamp: 1715003456}) {
		t.Fatalf("unexpected location: %+v", loc)
	}

	bad := []struct{ name, topic, payload string }{
		{"bad topic", "/fleet/vehicle/B1234XYZ", `{}`},
		{"not json", topic, `not-json`},
		{"missing latitude", topic, `{"vehicle_id":"B1234XYZ","longitude":106.8,"timestamp":1}`},
		{"topic mismatch", topic, `{"vehicle_id":"B5678ABC","latitude":-6.2,"longitude":106.8,"timestamp":1}`},
		{"latitude range", topic, `{"vehicle_id":"B1234XYZ","latitude":-91,"longitude":106.8,"timestamp":1}`},
		{"longitude range", topic, `{"vehicle_id":"B1234XYZ","latitude":-6.2,"longitude":181,"timestamp":1}`},
		{"timestamp zero", topic, `{"vehicle_id":"B1234XYZ","latitude":-6.2,"longitude":106.8,"timestamp":0}`},
		{"invalid vehicle id", "/fleet/vehicle/B 1/location", `{"vehicle_id":"B 1","latitude":-6.2,"longitude":106.8,"timestamp":1}`},
	}
	for _, tt := range bad {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseLocation(tt.topic, []byte(tt.payload)); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("err = %v, want ErrInvalidMessage", err)
			}
		})
	}
}

func TestParseAlert(t *testing.T) {
	msg := rabbitmq.Message{
		MessageId: "6f1c0d3e-0000-0000-0000-000000000000",
		Headers:   map[string]any{"geofence_id": "monas"},
		Body:      []byte(`{"vehicle_id":"B1234XYZ","event":"geofence_entry","location":{"latitude":-6.1754,"longitude":106.8272},"timestamp":1715003456}`),
	}
	alert, geofenceID, err := ParseAlert(msg)
	if err != nil {
		t.Fatalf("ParseAlert: %v", err)
	}
	if geofenceID != "monas" || alert.VehicleID != "B1234XYZ" || alert.Location.Latitude != -6.1754 {
		t.Fatalf("unexpected alert: %+v %s", alert, geofenceID)
	}

	noID := msg
	noID.MessageId = ""
	noHeader := msg
	noHeader.Headers = nil
	badBody := msg
	badBody.Body = []byte(`{`)
	for name, m := range map[string]rabbitmq.Message{"no message id": noID, "no geofence header": noHeader, "bad body": badBody} {
		if _, _, err := ParseAlert(m); !errors.Is(err, ErrInvalidMessage) {
			t.Errorf("%s: err = %v, want ErrInvalidMessage", name, err)
		}
	}
}

func TestRelayBackoff(t *testing.T) {
	r := &Relay{maxBackoff: 5 * time.Minute}
	for attempts, want := range map[int32]int32{0: 1, 1: 2, 3: 8, 8: 256, 9: 300, 40: 300} {
		if got := r.backoffSeconds(attempts); got != want {
			t.Errorf("backoffSeconds(%d) = %d, want %d", attempts, got, want)
		}
	}
}

func TestSimulatorVisitsGeofences(t *testing.T) {
	gs, _ := ParseGeofences("bundaran-hi:-6.1950:106.8230,monas:-6.1754:106.8272,blok-m:-6.2443:106.8000")
	sim, err := NewSimulator([]string{"B1234XYZ", "B5678ABC"}, gs, 10)
	if err != nil {
		t.Fatalf("NewSimulator: %v", err)
	}

	visited := map[string]map[string]bool{}
	now := time.Unix(1715003456, 0)
	for tick := 0; tick < 3*10; tick++ { // satu putaran penuh
		for _, loc := range sim.Next(now.Add(time.Duration(tick) * 2 * time.Second)) {
			if visited[loc.VehicleID] == nil {
				visited[loc.VehicleID] = map[string]bool{}
			}
			for _, g := range gs {
				if DistanceMeters(loc.Latitude, loc.Longitude, g.Latitude, g.Longitude) <= 50 {
					visited[loc.VehicleID][g.ID] = true
				}
			}
		}
	}
	for _, v := range []string{"B1234XYZ", "B5678ABC"} {
		if len(visited[v]) != len(gs) {
			t.Errorf("%s visited %v, want all %d geofences", v, visited[v], len(gs))
		}
	}

	if _, err := NewSimulator(nil, gs, 10); err == nil {
		t.Error("expected error without vehicles")
	}
	if _, err := NewSimulator([]string{"A"}, gs[:1], 10); err == nil {
		t.Error("expected error with one geofence")
	}
}
