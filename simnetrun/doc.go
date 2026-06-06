// Package simnetrun is the simnet backend's run entry point.
//
// simnet's timing is only valid under testing/synctest's virtual clock: a plain
// binary runs simnet on the OS clock, so its arrival delays measure goroutine /
// timer scheduling latency, not the network model (this is exactly why the old
// ./slot-sim binary read ~45% high on large blocks vs Shadow). And synctest runs
// only under `go test` (Go 1.26 exposes synctest.Test(*testing.T, …) only — no
// standalone Run). So the simnet "run" is a test, TestRun, driven by simctl via
// `go test -tags simnetrun`. It is build-tagged so the normal suite never runs
// it. See run_test.go.
package simnetrun
