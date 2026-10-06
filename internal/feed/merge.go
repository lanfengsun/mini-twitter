// Package feed holds the pure (no I/O) feed logic so it can be unit tested.
package feed

import (
	"sort"
	"strconv"
)

// MergeDesc merges several id lists into one list ordered newest first (ids are
// time-sortable), dropping duplicates and the 0 sentinel, and returns at most limit ids.
// This is the read-time merge of a user's pushed feed with the recent posts of the
// celebrities they follow (which are not fanned out).
func MergeDesc(limit int, lists ...[]int64) []int64 {
	total := 0
	for _, l := range lists {
		total += len(l)
	}
	all := make([]int64, 0, total)
	for _, l := range lists {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] > all[j] })
	out := make([]int64, 0, min(limit, len(all)))
	var last int64 = -1
	for _, id := range all {
		if id <= 0 || id == last {
			continue
		}
		out = append(out, id)
		last = id
		if len(out) == limit {
			break
		}
	}
	return out
}

// ParseIDs converts Redis string members to ids, skipping anything unparsable.
func ParseIDs(members []string) []int64 {
	out := make([]int64, 0, len(members))
	for _, m := range members {
		if id, err := strconv.ParseInt(m, 10, 64); err == nil {
			out = append(out, id)
		}
	}
	return out
}

// ParseCursor parses the pagination cursor (the id of the last post on the previous
// page). The empty string means "start from the newest".
func ParseCursor(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
}
