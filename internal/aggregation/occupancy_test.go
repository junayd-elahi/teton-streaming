package aggregation

import (
	"testing"
	"time"
)

var noon = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

func at(seconds int) time.Time { return noon.Add(time.Duration(seconds) * time.Second) }

func TestIntegrateOccupancy(t *testing.T) {
	absentBefore := &Transition{At: at(-30), InRoom: false}
	presentBefore := &Transition{At: at(-30), InRoom: true}
	cases := []struct {
		name        string
		predecessor *Transition
		transitions []Transition
		want        Occupancy
	}{
		{"absent, present at :10, absent at :40", absentBefore,
			[]Transition{{at(10), true}, {at(40), false}},
			Occupancy{InRoom: false, OccupiedSeconds: 30, KnownSeconds: 60}},
		{"late present at :50 added", absentBefore,
			[]Transition{{at(10), true}, {at(40), false}, {at(50), true}},
			Occupancy{InRoom: true, OccupiedSeconds: 40, KnownSeconds: 60}},
		{"occupied throughout, no transitions", presentBefore, nil,
			Occupancy{InRoom: true, OccupiedSeconds: 60, KnownSeconds: 60}},
		{"no history at all", nil, nil,
			Occupancy{InRoom: false, OccupiedSeconds: 0, KnownSeconds: 0}},
		{"no predecessor, present from :20", nil,
			[]Transition{{at(20), true}},
			Occupancy{InRoom: true, OccupiedSeconds: 40, KnownSeconds: 40}},
		{"repeated present does not double count", absentBefore,
			[]Transition{{at(10), true}, {at(20), true}, {at(40), false}},
			Occupancy{InRoom: false, OccupiedSeconds: 30, KnownSeconds: 60}},
		{"present dated 20s after the window end sets the state only", absentBefore,
			[]Transition{{at(10), true}, {at(40), false}, {at(80), true}},
			Occupancy{InRoom: true, OccupiedSeconds: 30, KnownSeconds: 60}},
		{"transition exactly at window end", absentBefore,
			[]Transition{{at(60), true}},
			Occupancy{InRoom: true, OccupiedSeconds: 0, KnownSeconds: 60}},
	}
	for _, c := range cases {
		got := IntegrateOccupancy(at(0), at(60), c.predecessor, c.transitions)
		if got != c.want {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
	}
}

func TestDenseFractionalTransitionsStayWithinTheExactWindow(t *testing.T) {
	previous := &Transition{At: at(-1), InRoom: true}
	transitions := make([]Transition, 3000)
	for i := range transitions {
		transitions[i] = Transition{
			At:     noon.Add(time.Duration(i+1) * 20 * time.Millisecond),
			InRoom: (i+1)%2 == 0,
		}
	}
	got := IntegrateOccupancy(noon, at(60), previous, transitions)
	if got.KnownSeconds != 60 || got.OccupiedSeconds != 30 || !got.InRoom {
		t.Fatalf("fractional transitions escaped exact window totals: %+v", got)
	}
}
