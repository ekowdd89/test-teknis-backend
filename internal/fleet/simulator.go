package fleet

import (
	"errors"
	"time"
)

// Simulator menghasilkan lokasi palsu untuk publisher (dev/demo).
// Setiap kendaraan berkeliling dari satu geofence ke geofence berikutnya
// dalam garis lurus, sehingga event geofence_entry muncul berkala.
type Simulator struct {
	vehicleIDs []string
	geofences  []Geofence
	steps      int
	// posisi tiap kendaraan: indeks geofence asal & langkah ke-berapa
	leg  []int
	step []int
}

func NewSimulator(vehicleIDs []string, geofences []Geofence, stepsPerLeg int) (*Simulator, error) {
	if len(vehicleIDs) == 0 {
		return nil, errors.New("fleet: at least one vehicle id is required")
	}
	if len(geofences) < 2 {
		return nil, errors.New("fleet: at least two geofences are required")
	}
	if stepsPerLeg < 2 {
		return nil, errors.New("fleet: steps per leg must be >= 2")
	}
	s := &Simulator{
		vehicleIDs: vehicleIDs,
		geofences:  geofences,
		steps:      stepsPerLeg,
		leg:        make([]int, len(vehicleIDs)),
		step:       make([]int, len(vehicleIDs)),
	}
	// Kendaraan mulai dari geofence berbeda agar event tidak bersamaan.
	for i := range vehicleIDs {
		s.leg[i] = i % len(geofences)
	}
	return s, nil
}

// Next mengembalikan posisi berikutnya untuk semua kendaraan.
func (s *Simulator) Next(now time.Time) []VehicleLocation {
	locs := make([]VehicleLocation, len(s.vehicleIDs))
	for i, id := range s.vehicleIDs {
		from := s.geofences[s.leg[i]]
		to := s.geofences[(s.leg[i]+1)%len(s.geofences)]
		t := float64(s.step[i]) / float64(s.steps)
		locs[i] = VehicleLocation{
			VehicleID: id,
			Latitude:  from.Latitude + (to.Latitude-from.Latitude)*t,
			Longitude: from.Longitude + (to.Longitude-from.Longitude)*t,
			Timestamp: now.Unix(),
		}

		s.step[i]++
		if s.step[i] >= s.steps {
			s.step[i] = 0
			s.leg[i] = (s.leg[i] + 1) % len(s.geofences)
		}
	}
	return locs
}
