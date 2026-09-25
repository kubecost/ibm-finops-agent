package cldy

import "time"

// Quarantine bounds: past either, the oldest quarantined items are evicted and counted as drops.
// They are variables so tests can shrink them.
var (
	quarantineMaxBytes int64 = 100 << 20
	quarantineMaxAge         = 7 * 24 * time.Hour
)
