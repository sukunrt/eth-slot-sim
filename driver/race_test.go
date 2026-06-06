//go:build race

package driver_test

// raceEnabled is true when built with -race. The large N-node synctest run
// overflows ThreadSanitizer's internal epoch limit (a TSan limitation, not a
// data race), so that one test skips under -race; the concurrency-sensitive
// paths stay covered by the metrics concurrent test and the node milestone.
const raceEnabled = true
