package fleet

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const earthRadiusMeters = 6371000.0

type Geofence struct {
	ID        string
	Latitude  float64
	Longitude float64
}

// ParseGeofences membaca format GEOFENCE_POINTS: "nama:lat:lon,nama:lat:lon".
func ParseGeofences(s string) (gs []Geofence, err error) {
	seen := make(map[string]bool)
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.Split(item, ":")
		if len(parts) != 3 || parts[0] == "" {
			return nil, fmt.Errorf("geofence %q: format must be name:lat:lon", item)
		}
		lat, err := strconv.ParseFloat(parts[1], 64)
		if err != nil || lat < -90 || lat > 90 {
			return nil, fmt.Errorf("geofence %q: invalid latitude", item)
		}
		lon, err := strconv.ParseFloat(parts[2], 64)
		if err != nil || lon < -180 || lon > 180 {
			return nil, fmt.Errorf("geofence %q: invalid longitude", item)
		}
		if seen[parts[0]] {
			return nil, fmt.Errorf("geofence %q: duplicate name", parts[0])
		}
		seen[parts[0]] = true
		gs = append(gs, Geofence{ID: parts[0], Latitude: lat, Longitude: lon})
	}
	return gs, nil
}

// DistanceMeters menghitung jarak dua titik dengan rumus haversine.
func DistanceMeters(lat1, lon1, lat2, lon2 float64) float64 {
	rad := math.Pi / 180
	dLat := (lat2 - lat1) * rad
	dLon := (lon2 - lon1) * rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusMeters * math.Asin(math.Sqrt(a))
}
