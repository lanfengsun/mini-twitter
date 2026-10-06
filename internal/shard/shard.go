// Package shard decides which Postgres shard owns a key.
package shard

import "hash/fnv"

// ForString routes string keys (usernames).
func ForString(s string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return int(h.Sum32() % uint32(n))
}

// ForID routes numeric ids. Ids are time-ordered, so they are mixed (splitmix64 finaliser)
// before taking the modulus; otherwise consecutive ids would map to shards in lockstep.
func ForID(id int64, n int) int {
	x := uint64(id)
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return int(x % uint64(n))
}
