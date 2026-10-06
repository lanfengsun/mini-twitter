package shard

import (
	"fmt"
	"testing"
)

func checkEven(t *testing.T, name string, counts []int, total int) {
	t.Helper()
	want := float64(total) / float64(len(counts))
	for i, c := range counts {
		if dev := (float64(c) - want) / want; dev > 0.05 || dev < -0.05 {
			t.Errorf("%s: shard %d has %d rows, %.1f%% off the mean", name, i, c, dev*100)
		}
	}
}

func TestForIDSpreadsSequentialIDs(t *testing.T) {
	counts := make([]int, 3)
	const total = 300_000
	for i := 0; i < total; i++ {
		counts[ForID(int64(1_000_000+i), 3)]++
	}
	checkEven(t, "sequential ids", counts, total)
}

func TestForIDSpreadsTimeSortedIDs(t *testing.T) {
	// ids that step by 4096 (one millisecond) look very non-random to a naive modulus
	counts := make([]int, 3)
	const total = 300_000
	for i := 0; i < total; i++ {
		counts[ForID(int64(i)<<12, 3)]++
	}
	checkEven(t, "ms-stepped ids", counts, total)
}

func TestForStringSpreadsUsernames(t *testing.T) {
	counts := make([]int, 3)
	const total = 300_000
	for i := 0; i < total; i++ {
		counts[ForString(fmt.Sprintf("u%d", i), 3)]++
	}
	checkEven(t, "usernames", counts, total)
}

func TestStableAndInRange(t *testing.T) {
	for n := 1; n <= 8; n++ {
		for i := int64(0); i < 1000; i++ {
			s := ForID(i, n)
			if s < 0 || s >= n || s != ForID(i, n) {
				t.Fatalf("bad route %d for id %d n=%d", s, i, n)
			}
		}
	}
}
