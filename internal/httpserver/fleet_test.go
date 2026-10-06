package httpserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ekowdd89/test-teknis-backend/internal/httpserver/openapi"
	"github.com/ekowdd89/test-teknis-backend/internal/postgres/sqlc"
)

// fakeQuerier hanya mengimplementasikan query yang dipakai handler;
// method lain akan panic karena interface embedded bernilai nil.
type fakeQuerier struct {
	sqlc.Querier

	latest      map[string]sqlc.GetLatestVehicleLocationRow
	history     []sqlc.ListVehicleLocationHistoryRow
	events      []sqlc.GeofenceEvent
	err         error
	lastHistory sqlc.ListVehicleLocationHistoryParams
	lastEvents  sqlc.ListGeofenceEventsByVehicleParams
}

func (f *fakeQuerier) GetLatestVehicleLocation(_ context.Context, vehicleID string) (sqlc.GetLatestVehicleLocationRow, error) {
	if f.err != nil {
		return sqlc.GetLatestVehicleLocationRow{}, f.err
	}
	row, ok := f.latest[vehicleID]
	if !ok {
		return row, sql.ErrNoRows
	}
	return row, nil
}

func (f *fakeQuerier) ListVehicleLocationHistory(_ context.Context, arg sqlc.ListVehicleLocationHistoryParams) ([]sqlc.ListVehicleLocationHistoryRow, error) {
	f.lastHistory = arg
	return f.history, f.err
}

func (f *fakeQuerier) ListGeofenceEventsByVehicle(_ context.Context, arg sqlc.ListGeofenceEventsByVehicleParams) ([]sqlc.GeofenceEvent, error) {
	f.lastEvents = arg
	return f.events, f.err
}

func newTestServer(t *testing.T, q sqlc.Querier, opts ...OptFunc) http.Handler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	opts = append([]OptFunc{WithQuerier(q), WithLogger(logger)}, opts...)
	hs, err := New(opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return hs.Handler()
}

func do(t *testing.T, h http.Handler, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return v
}

func TestNewRequiresQuerier(t *testing.T) {
	if _, err := New(); err == nil {
		t.Fatal("expected error without querier")
	}
}

func TestGetVehicleLocation(t *testing.T) {
	q := &fakeQuerier{latest: map[string]sqlc.GetLatestVehicleLocationRow{
		"B1234XYZ": {VehicleID: "B1234XYZ", Latitude: -6.2088, Longitude: 106.8456, Timestamp: 1715003456},
	}}
	h := newTestServer(t, q)

	rec := do(t, h, "/vehicles/B1234XYZ/location")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	loc := decode[openapi.VehicleLocation](t, rec)
	if loc.VehicleId != "B1234XYZ" || loc.Latitude != -6.2088 || loc.Timestamp != 1715003456 {
		t.Fatalf("unexpected location: %+v", loc)
	}

	tests := []struct {
		name   string
		target string
		status int
		code   string
	}{
		{"not found", "/vehicles/B0000AAA/location", http.StatusNotFound, codeNotFound},
		{"invalid char", "/vehicles/B_1234/location", http.StatusBadRequest, codeInvalidArgument},
		{"too long", "/vehicles/ABCDEFGHIJKLMNOPQRSTU/location", http.StatusBadRequest, codeInvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, h, tt.target)
			if rec.Code != tt.status {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tt.status, rec.Body)
			}
			if e := decode[openapi.Error](t, rec); e.Code != tt.code {
				t.Fatalf("code = %q, want %q", e.Code, tt.code)
			}
		})
	}
}

func TestInternalErrorHidesDetail(t *testing.T) {
	h := newTestServer(t, &fakeQuerier{err: errors.New("pq: connection refused to 10.0.0.1")})

	for _, target := range []string{
		"/vehicles/B1234XYZ/location",
		"/vehicles/B1234XYZ/history?start=1&end=2",
		"/vehicles/B1234XYZ/geofence-events",
	} {
		rec := do(t, h, target)
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("%s: status = %d", target, rec.Code)
		}
		if e := decode[openapi.Error](t, rec); e.Code != codeInternal || e.Message != msgInternal {
			t.Fatalf("%s: unexpected error body %+v", target, e)
		}
	}
}

func TestGetVehicleHistory(t *testing.T) {
	q := &fakeQuerier{history: []sqlc.ListVehicleLocationHistoryRow{
		{VehicleID: "B1234XYZ", Latitude: -6.2088, Longitude: 106.8456, Timestamp: 1715003456},
		{VehicleID: "B1234XYZ", Latitude: -6.2079, Longitude: 106.8461, Timestamp: 1715003458},
	}}
	h := newTestServer(t, q)

	rec := do(t, h, "/vehicles/B1234XYZ/history?start=1715000000&end=1715009999")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	if locs := decode[[]openapi.VehicleLocation](t, rec); len(locs) != 2 {
		t.Fatalf("len = %d, want 2", len(locs))
	}
	want := sqlc.ListVehicleLocationHistoryParams{VehicleID: "B1234XYZ", StartTs: 1715000000, EndTs: 1715009999, MaxRows: defaultHistoryLimit}
	if q.lastHistory != want {
		t.Fatalf("params = %+v, want %+v", q.lastHistory, want)
	}

	rec = do(t, h, "/vehicles/B1234XYZ/history?start=1&end=2&limit=10")
	if rec.Code != http.StatusOK || q.lastHistory.MaxRows != 10 {
		t.Fatalf("status = %d, limit = %d", rec.Code, q.lastHistory.MaxRows)
	}

	bad := []struct{ name, target string }{
		{"start > end", "/vehicles/B1234XYZ/history?start=10&end=5"},
		{"negative", "/vehicles/B1234XYZ/history?start=-1&end=5"},
		{"missing start", "/vehicles/B1234XYZ/history?end=5"},
		{"missing end", "/vehicles/B1234XYZ/history?start=5"},
		{"not a number", "/vehicles/B1234XYZ/history?start=abc&end=5"},
		{"limit zero", "/vehicles/B1234XYZ/history?start=1&end=5&limit=0"},
		{"limit too big", "/vehicles/B1234XYZ/history?start=1&end=5&limit=10001"},
	}
	for _, tt := range bad {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, h, tt.target)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body)
			}
			if e := decode[openapi.Error](t, rec); e.Code != codeInvalidArgument {
				t.Fatalf("code = %q", e.Code)
			}
		})
	}
}

func TestGetVehicleHistoryEmptyIsArray(t *testing.T) {
	h := newTestServer(t, &fakeQuerier{history: []sqlc.ListVehicleLocationHistoryRow{}})

	rec := do(t, h, "/vehicles/B1234XYZ/history?start=1&end=2")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	// Spesifikasi: tanpa data dikembalikan array kosong, bukan null.
	if body := rec.Body.String(); body != "[]\n" {
		t.Fatalf("body = %q, want []", body)
	}
}

func TestListVehicleGeofenceEvents(t *testing.T) {
	receivedAt := time.Date(2026, 10, 6, 8, 0, 0, 0, time.UTC)
	q := &fakeQuerier{events: []sqlc.GeofenceEvent{{
		VehicleID: "B1234XYZ", GeofenceID: "monas", EventType: "geofence_entry",
		Latitude: -6.1754, Longitude: 106.8272, Timestamp: 1715003456, ReceivedAt: receivedAt,
	}}}
	h := newTestServer(t, q)

	rec := do(t, h, "/vehicles/B1234XYZ/geofence-events?limit=5")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	if q.lastEvents.MaxRows != 5 {
		t.Fatalf("limit = %d, want 5", q.lastEvents.MaxRows)
	}
	events := decode[[]openapi.GeofenceEvent](t, rec)
	ev := events[0]
	if ev.GeofenceId != "monas" || ev.Event != "geofence_entry" || ev.Location.Latitude != -6.1754 || !ev.ReceivedAt.Equal(receivedAt) {
		t.Fatalf("unexpected event: %+v", ev)
	}

	do(t, h, "/vehicles/B1234XYZ/geofence-events")
	if q.lastEvents.MaxRows != defaultEventsLimit {
		t.Fatalf("default limit = %d, want %d", q.lastEvents.MaxRows, defaultEventsLimit)
	}

	if rec := do(t, h, "/vehicles/B1234XYZ/geofence-events?limit=1001"); rec.Code != http.StatusBadRequest {
		t.Fatalf("limit too big: status = %d", rec.Code)
	}
}

func TestGetHealth(t *testing.T) {
	up := func(context.Context) error { return nil }
	down := func(context.Context) error { return errors.New("connection refused") }

	tests := []struct {
		name   string
		opts   []OptFunc
		status int
		want   openapi.Health
	}{
		{
			name:   "all up",
			opts:   []OptFunc{WithHealthCheck("postgres", up), WithHealthCheck("rabbitmq", up)},
			status: http.StatusOK,
			want:   openapi.Health{Status: openapi.Ok, Checks: map[string]openapi.HealthChecks{"postgres": openapi.Up, "rabbitmq": openapi.Up}},
		},
		{
			name:   "one down",
			opts:   []OptFunc{WithHealthCheck("postgres", up), WithHealthCheck("rabbitmq", down)},
			status: http.StatusServiceUnavailable,
			want:   openapi.Health{Status: openapi.Degraded, Checks: map[string]openapi.HealthChecks{"postgres": openapi.Up, "rabbitmq": openapi.Down}},
		},
		{
			name:   "no checks",
			status: http.StatusOK,
			want:   openapi.Health{Status: openapi.Ok, Checks: map[string]openapi.HealthChecks{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, newTestServer(t, &fakeQuerier{}, tt.opts...), "/healthz")
			if rec.Code != tt.status {
				t.Fatalf("status = %d, want %d", rec.Code, tt.status)
			}
			got := decode[openapi.Health](t, rec)
			if got.Status != tt.want.Status || len(got.Checks) != len(tt.want.Checks) {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
			for k, v := range tt.want.Checks {
				if got.Checks[k] != v {
					t.Fatalf("check %s = %q, want %q", k, got.Checks[k], v)
				}
			}
		})
	}
}

func TestHealthCheckTimeout(t *testing.T) {
	// Dependensi yang menggantung tidak boleh membuat /healthz menggantung.
	hang := func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	hs, err := New(
		WithQuerier(&fakeQuerier{}),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		WithHealthCheck("postgres", hang),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	hs.healthCheckTimeout = 50 * time.Millisecond

	start := time.Now()
	rec := do(t, hs.Handler(), "/healthz")
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("health check took %v", elapsed)
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestServeGracefulShutdown(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	hs, err := New(
		WithListener(lis),
		WithQuerier(&fakeQuerier{}),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- hs.Serve(ctx) }()

	// Tunggu listener siap, lalu pastikan server benar-benar melayani request.
	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := http.Get("http://" + lis.Addr().String() + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d", resp.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return after cancel")
	}
}

func TestUnknownRoute(t *testing.T) {
	rec := do(t, newTestServer(t, &fakeQuerier{}), "/does-not-exist")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d", rec.Code)
	}
	if e := decode[openapi.Error](t, rec); e.Code != codeNotFound {
		t.Fatalf("code = %q", e.Code)
	}
}
