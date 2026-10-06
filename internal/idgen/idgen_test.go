package idgen

import (
	"testing"
	"time"
)

func TestUniqueIncreasingAndSafeInteger(t *testing.T) {
	g := New(3)
	var prev int64
	seen := make(map[int64]struct{}, 1_000_000)
	for i := 0; i < 1_000_000; i++ {
		id := g.Next()
		if id <= prev {
			t.Fatalf("id %d not greater than previous %d at i=%d", id, prev, i)
		}
		if id >= 1<<53 {
			t.Fatalf("id %d does not fit in 53 bits", id)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id %d", id)
		}
		seen[id] = struct{}{}
		prev = id
	}
}

func TestClockGoingBackwardsStaysMonotonic(t *testing.T) {
	g := New(1)
	ms := time.Now().UnixMilli()
	g.now = func() int64 { return ms }
	a := g.Next()
	g.now = func() int64 { return ms - 5000 }
	b := g.Next()
	if b <= a {
		t.Fatalf("expected %d > %d after clock moved backwards", b, a)
	}
}

func TestTimeRoundTrip(t *testing.T) {
	want := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	id := Make(want.UnixMilli(), 7, 9)
	if got := Time(id); !got.Equal(want) {
		t.Fatalf("Time(Make(%v)) = %v", want, got)
	}
	if MinIDAt(want) > id {
		t.Fatal("MinIDAt must not exceed an id minted at the same instant")
	}
}

func TestNodesNeverCollide(t *testing.T) {
	a, b := New(1), New(2)
	ms := time.Now().UnixMilli()
	a.now = func() int64 { return ms }
	b.now = func() int64 { return ms }
	if a.Next() == b.Next() {
		t.Fatal("different nodes minted the same id")
	}
}
