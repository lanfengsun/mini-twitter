package feed

import (
	"reflect"
	"testing"
)

func TestMergeDesc(t *testing.T) {
	cases := []struct {
		name  string
		limit int
		lists [][]int64
		want  []int64
	}{
		{"interleaves newest first", 5, [][]int64{{9, 5, 1}, {8, 6, 2}}, []int64{9, 8, 6, 5, 2}},
		{"dedupes across lists", 10, [][]int64{{9, 5}, {9, 5, 3}}, []int64{9, 5, 3}},
		{"drops the zero sentinel", 10, [][]int64{{4, 0}, {0}}, []int64{4}},
		{"respects limit", 2, [][]int64{{9, 8, 7}}, []int64{9, 8}},
		{"empty inputs", 5, [][]int64{nil, {}}, []int64{}},
		{"no lists", 5, nil, []int64{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := MergeDesc(c.limit, c.lists...)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got %v want %v", got, c.want)
			}
		})
	}
}

func TestMergeTopLimitComesFromPerSourceTopLimit(t *testing.T) {
	// The feed reader asks every source for only `limit` items. This checks that the
	// top `limit` of the merge is the same as the top `limit` of the full union.
	a := []int64{100, 90, 80, 70, 60, 50}
	b := []int64{95, 85, 75, 65, 55}
	full := MergeDesc(4, a, b)
	partial := MergeDesc(4, a[:4], b[:4])
	if !reflect.DeepEqual(full, partial) {
		t.Fatalf("full %v != partial %v", full, partial)
	}
}

func TestParseIDsAndCursor(t *testing.T) {
	if got := ParseIDs([]string{"3", "x", "0", "12"}); !reflect.DeepEqual(got, []int64{3, 0, 12}) {
		t.Fatalf("got %v", got)
	}
	if c, err := ParseCursor(""); c != 0 || err != nil {
		t.Fatalf("empty cursor: %d %v", c, err)
	}
	if c, err := ParseCursor("42"); c != 42 || err != nil {
		t.Fatalf("cursor: %d %v", c, err)
	}
	if _, err := ParseCursor("nope"); err == nil {
		t.Fatal("expected error")
	}
}
