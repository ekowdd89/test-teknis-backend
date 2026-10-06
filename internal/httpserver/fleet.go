package httpserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sync"

	"github.com/ekowdd89/test-teknis-backend/internal/httpserver/openapi"
	"github.com/ekowdd89/test-teknis-backend/internal/postgres/sqlc"
)

// Batas & default disamakan dengan api/openapi-spec.yaml.
const (
	defaultHistoryLimit = 1000
	maxHistoryLimit     = 10000
	defaultEventsLimit  = 100
	maxEventsLimit      = 1000
)

const (
	codeInvalidArgument = "INVALID_ARGUMENT"
	codeNotFound        = "NOT_FOUND"
	codeInternal        = "INTERNAL"
	msgInternal         = "internal server error"
)

var vehicleIDPattern = regexp.MustCompile(`^[A-Za-z0-9-]{1,20}$`)

// GetHealth implements openapi.StrictServerInterface.
func (o *openapiServerImplementation) GetHealth(ctx context.Context, request openapi.GetHealthRequestObject) (openapi.GetHealthResponseObject, error) {
	health := openapi.Health{
		Status: openapi.Ok,
		Checks: make(map[string]openapi.HealthChecks, len(o.h.checks)),
	}

	// Semua dependensi dicek paralel agar total waktu = check paling lambat.
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for name, check := range o.h.checks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			checkCtx, cancel := context.WithTimeout(ctx, o.h.healthCheckTimeout)
			defer cancel()

			state := openapi.Up
			if err := check(checkCtx); err != nil {
				state = openapi.Down
				o.h.logger.WarnContext(ctx, "health check failed", "check", name, "error", err)
			}
			mu.Lock()
			health.Checks[name] = state
			if state == openapi.Down {
				health.Status = openapi.Degraded
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if health.Status != openapi.Ok {
		return openapi.GetHealth503JSONResponse(health), nil
	}
	return openapi.GetHealth200JSONResponse(health), nil
}

// GetVehicleHistory implements openapi.StrictServerInterface.
func (o *openapiServerImplementation) GetVehicleHistory(ctx context.Context, request openapi.GetVehicleHistoryRequestObject) (openapi.GetVehicleHistoryResponseObject, error) {
	if err := validateVehicleID(request.VehicleId); err != nil {
		return openapi.GetVehicleHistory400JSONResponse{BadRequestJSONResponse: badRequest(err)}, nil
	}
	params := request.Params
	if params.Start < 0 || params.End < 0 {
		return openapi.GetVehicleHistory400JSONResponse{BadRequestJSONResponse: badRequest(errors.New("start and end must be >= 0"))}, nil
	}
	if params.Start > params.End {
		return openapi.GetVehicleHistory400JSONResponse{BadRequestJSONResponse: badRequest(errors.New("start must be less than or equal to end"))}, nil
	}
	limit, err := resolveLimit(params.Limit, defaultHistoryLimit, maxHistoryLimit)
	if err != nil {
		return openapi.GetVehicleHistory400JSONResponse{BadRequestJSONResponse: badRequest(err)}, nil
	}

	rows, err := o.h.queries.ListVehicleLocationHistory(ctx, sqlc.ListVehicleLocationHistoryParams{
		VehicleID: request.VehicleId,
		StartTs:   params.Start,
		EndTs:     params.End,
		MaxRows:   limit,
	})
	if err != nil {
		return openapi.GetVehicleHistory500JSONResponse{InternalErrorJSONResponse: o.internalError(ctx, err)}, nil
	}

	resp := make(openapi.GetVehicleHistory200JSONResponse, 0, len(rows))
	for _, r := range rows {
		resp = append(resp, openapi.VehicleLocation{
			VehicleId: r.VehicleID,
			Latitude:  r.Latitude,
			Longitude: r.Longitude,
			Timestamp: r.Timestamp,
		})
	}
	return resp, nil
}

// GetVehicleLocation implements openapi.StrictServerInterface.
func (o *openapiServerImplementation) GetVehicleLocation(ctx context.Context, request openapi.GetVehicleLocationRequestObject) (openapi.GetVehicleLocationResponseObject, error) {
	if err := validateVehicleID(request.VehicleId); err != nil {
		return openapi.GetVehicleLocation400JSONResponse{BadRequestJSONResponse: badRequest(err)}, nil
	}

	row, err := o.h.queries.GetLatestVehicleLocation(ctx, request.VehicleId)
	if errors.Is(err, sql.ErrNoRows) {
		return openapi.GetVehicleLocation404JSONResponse{NotFoundJSONResponse: openapi.NotFoundJSONResponse{
			Code:    codeNotFound,
			Message: fmt.Sprintf("vehicle %s not found", request.VehicleId),
		}}, nil
	}
	if err != nil {
		return openapi.GetVehicleLocation500JSONResponse{InternalErrorJSONResponse: o.internalError(ctx, err)}, nil
	}

	return openapi.GetVehicleLocation200JSONResponse{
		VehicleId: row.VehicleID,
		Latitude:  row.Latitude,
		Longitude: row.Longitude,
		Timestamp: row.Timestamp,
	}, nil
}

// ListVehicleGeofenceEvents implements openapi.StrictServerInterface.
func (o *openapiServerImplementation) ListVehicleGeofenceEvents(ctx context.Context, request openapi.ListVehicleGeofenceEventsRequestObject) (openapi.ListVehicleGeofenceEventsResponseObject, error) {
	if err := validateVehicleID(request.VehicleId); err != nil {
		return openapi.ListVehicleGeofenceEvents400JSONResponse{BadRequestJSONResponse: badRequest(err)}, nil
	}
	limit, err := resolveLimit(request.Params.Limit, defaultEventsLimit, maxEventsLimit)
	if err != nil {
		return openapi.ListVehicleGeofenceEvents400JSONResponse{BadRequestJSONResponse: badRequest(err)}, nil
	}

	rows, err := o.h.queries.ListGeofenceEventsByVehicle(ctx, sqlc.ListGeofenceEventsByVehicleParams{
		VehicleID: request.VehicleId,
		MaxRows:   limit,
	})
	if err != nil {
		return openapi.ListVehicleGeofenceEvents500JSONResponse{InternalErrorJSONResponse: o.internalError(ctx, err)}, nil
	}

	resp := make(openapi.ListVehicleGeofenceEvents200JSONResponse, 0, len(rows))
	for _, r := range rows {
		resp = append(resp, openapi.GeofenceEvent{
			VehicleId:  r.VehicleID,
			GeofenceId: r.GeofenceID,
			Event:      r.EventType,
			Location:   openapi.Location{Latitude: r.Latitude, Longitude: r.Longitude},
			Timestamp:  r.Timestamp,
			ReceivedAt: r.ReceivedAt,
		})
	}
	return resp, nil
}

func validateVehicleID(id string) error {
	if !vehicleIDPattern.MatchString(id) {
		return errors.New("vehicle_id must be 1-20 characters of [A-Za-z0-9-]")
	}
	return nil
}

// resolveLimit: nil = default, selain itu harus 1..max.
func resolveLimit(limit *int32, def, max int32) (int32, error) {
	if limit == nil {
		return def, nil
	}
	if *limit < 1 || *limit > max {
		return 0, fmt.Errorf("limit must be between 1 and %d", max)
	}
	return *limit, nil
}

func badRequest(err error) openapi.BadRequestJSONResponse {
	return openapi.BadRequestJSONResponse{Code: codeInvalidArgument, Message: err.Error()}
}

// internalError mencatat error asli ke log, tetapi menyembunyikan
// detail error database dari client.
func (o *openapiServerImplementation) internalError(ctx context.Context, err error) openapi.InternalErrorJSONResponse {
	o.h.logger.ErrorContext(ctx, "query failed", "error", err)
	return openapi.InternalErrorJSONResponse{Code: codeInternal, Message: msgInternal}
}
