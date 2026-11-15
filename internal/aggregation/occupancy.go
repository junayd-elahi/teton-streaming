// Package aggregation holds the pure window calculations, free of storage and HTTP.
package aggregation

import "time"

// Transition is one presence event: the room state from At onwards.
type Transition struct {
	At     time.Time
	InRoom bool
}

// Occupancy is the result of integrating room state over a window.
type Occupancy struct {
	InRoom          bool    // state at the end of the window
	OccupiedSeconds float64 // time known to be occupied
	KnownSeconds    float64 // time with a known state, occupied or not
}

// IntegrateOccupancy computes occupied and known time over (start, end].
//
// predecessor is the last transition at or before start, or nil when the room
// has no earlier history; the span before the first transition is then unknown
// and counts as neither occupied nor absent. transitions must lie in
// (start, end] and be in event-time order.
func IntegrateOccupancy(start, end time.Time, predecessor *Transition, transitions []Transition) Occupancy {
	var result Occupancy
	known := predecessor != nil
	inRoom := known && predecessor.InRoom
	cursor := start

	advance := func(to time.Time) {
		span := to.Sub(cursor).Seconds()
		if known {
			result.KnownSeconds += span
			if inRoom {
				result.OccupiedSeconds += span
			}
		}
		cursor = to
	}
	for _, t := range transitions {
		advance(t.At)
		known, inRoom = true, t.InRoom
	}
	advance(end)
	result.InRoom = inRoom
	return result
}
