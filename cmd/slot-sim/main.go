// Command slot-sim runs the block-dissemination scenario on an in-process
// simnet under the real (wall) clock and prints the per-node block-arrival CDF.
// Tests under testing/synctest are where exact, instant numbers come from; this
// binary is for runs by hand at smaller N. Slot duration defaults short so a
// wall-clock run finishes in seconds, not minutes.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ethp2p/slot-sim/driver"
	"github.com/ethp2p/slot-sim/metrics"
	"github.com/ethp2p/slot-sim/netsim"
)

func main() {
	var (
		n         = flag.Int("n", 25, "number of nodes")
		slots     = flag.Int("slots", 0, "slots to run (0 = n, so every node proposes once)")
		slotDur   = flag.Duration("slot", time.Second, "slot duration (wall clock)")
		blockSize = flag.Int("block-size", 128*1024, "block size in bytes")
		peers     = flag.Int("peers", 20, "target peers per node")
		seed      = flag.Uint64("seed", 1, "rng seed")
		csvPath   = flag.String("csv", "", "optional path to write the arrival CDF as CSV")
	)
	flag.Parse()

	numSlots := *slots
	if numSlots == 0 {
		numSlots = *n
	}

	nw, err := netsim.New(netsim.Config{
		N: *n, P: *peers, SuperFrac: 0.20, Seed: *seed,
		MinLatency: 10 * time.Millisecond, MaxLatency: 150 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("netsim: %v", err)
	}
	defer nw.Close()

	rec := metrics.NewRecorder()
	d := driver.New(nw, driver.Config{
		BlockSize:    *blockSize,
		SlotDuration: *slotDur,
		Jitter:       *slotDur / 4, // spread proposals within the slot, never past its end
		VerifyDelay:  func() time.Duration { return 10 * time.Millisecond },
		D:            8, Dlo: 6, Dhi: 12,
		Seed: *seed,
	}, rec)

	ctx := context.Background()
	log.Printf("starting %d nodes, %d slots @ %v", *n, numSlots, *slotDur)
	if err := d.BringUp(ctx); err != nil {
		log.Fatal(err)
	}

	d.Run(ctx, time.Now(), numSlots)

	report(rec, *csvPath)
}

func report(rec *metrics.Recorder, csvPath string) {
	arr := rec.Arrivals()
	delays := make([]time.Duration, len(arr))
	for i, a := range arr {
		delays[i] = a.Delay
	}
	s := metrics.Summarize(delays)
	fmt.Printf("\nblock-arrival CDF over %d arrivals:\n", s.Count)
	fmt.Printf("  p50  = %v\n  p90  = %v\n  p99  = %v\n  p100 = %v\n", s.P50, s.P90, s.P99, s.P100)

	if csvPath == "" {
		return
	}
	f, err := os.Create(csvPath)
	if err != nil {
		log.Fatalf("create csv: %v", err)
	}
	defer f.Close()
	if err := rec.WriteCSV(f); err != nil {
		log.Fatalf("write csv: %v", err)
	}
	fmt.Printf("wrote %d rows to %s\n", s.Count, csvPath)
}
