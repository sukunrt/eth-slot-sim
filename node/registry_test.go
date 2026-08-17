package node

import (
	"testing"

	"github.com/ethp2p/slot-sim/pb"
	"github.com/ethp2p/slot-sim/validator"
)

// The numeric Kind values are a wire contract: they land in the arrivals CSV and the slog
// tracer, and analysis/check_arrivals.py hardcodes them. Renumbering breaks every analyzer.
func TestKindValuesPinned(t *testing.T) {
	pinned := map[Kind]int{
		KindBlock:             1,
		KindAttestation:       2,
		KindAggregate:         3,
		KindColumn:            4,
		KindSyncMessage:       5,
		KindSyncContribution:  6,
		KindACVote:            7,
		KindFinalityVote:      8,
		KindFinalityAggregate: 9,
		KindConsensusBlock:    10,
		KindExecutionPayload:  11,
		KindPTCVote:           12,
	}
	if len(pinned) != len(registry) {
		t.Fatalf("registry has %d kinds, pinned table has %d", len(registry), len(pinned))
	}
	for k, want := range pinned {
		if int(k) != want {
			t.Errorf("kind %d, want %d (Python/CSV contract)", int(k), want)
		}
	}
}

// buildLookups must reject malformed or ambiguous tables at init, so a bad registry entry
// fails the first test run instead of silently mis-routing a topic.
func TestBuildLookupsRejectsBadTables(t *testing.T) {
	dec := decodeAs(func(b *pb.Block) Identity { return Identity{} })
	good := func() descriptor {
		return descriptor{kind: KindBlock, topic: "/t/block", class: vcFixed, decode: dec}
	}
	cases := []struct {
		name string
		regs []descriptor
	}{
		{"duplicate kind", []descriptor{good(), good()}},
		{"empty class", []descriptor{{kind: KindBlock, topic: "/t/block", decode: dec}}},
		{"both topic and prefix", []descriptor{
			{kind: KindBlock, topic: "/t/block", prefix: "/t/b", class: vcFixed, decode: dec}}},
		{"neither topic nor prefix", []descriptor{{kind: KindBlock, class: vcFixed, decode: dec}}},
		{"duplicate exact topic", []descriptor{good(),
			{kind: KindAttestation, topic: "/t/block", class: vcConsensus, decode: dec}}},
		{"prefix prefixes prefix", []descriptor{
			{kind: KindBlock, prefix: "/t/a_", class: vcFixed, decode: dec},
			{kind: KindAttestation, prefix: "/t/a", class: vcConsensus, decode: dec}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("buildLookups accepted a table with %s", c.name)
				}
			}()
			buildLookups(c.regs)
		})
	}
}

// An exact topic may share a stem with a prefix family — the real sync contribution topic
// starts with the sync message prefix — and the exact match must win, by rule rather than
// case ordering. Tested against the live registry.
func TestExactTopicBeatsPrefix(t *testing.T) {
	if d := descriptorFor(validator.SyncContributionTopic); d == nil || d.kind != KindSyncContribution {
		t.Fatalf("contribution topic resolved to %+v, want KindSyncContribution", d)
	}
	if d := descriptorFor(validator.SyncMessageTopic(3)); d == nil || d.kind != KindSyncMessage {
		t.Fatalf("sync message topic resolved to %+v, want KindSyncMessage", d)
	}
	if d := descriptorFor("/eth2/never/registered"); d != nil {
		t.Fatalf("unknown topic resolved to %+v, want nil", d)
	}
}

// partialKindFor resolves a topic to its partial-kind facts — only the two partial-capable
// prefix families match (partial-attestation-spec.md §0: everything else stays classic), and
// the subnet parses out of the topic name.
func TestPartialKindFor(t *testing.T) {
	if kind, subnet, ok := partialKindFor(validator.AttestationTopic(7)); !ok ||
		kind != KindAttestation || subnet != 7 {
		t.Fatalf("attestation topic = (%d, %d, %v), want (KindAttestation, 7, true)", kind, subnet, ok)
	}
	if kind, subnet, ok := partialKindFor(validator.FinalityVoteTopic(3)); !ok ||
		kind != KindFinalityVote || subnet != 3 {
		t.Fatalf("finality topic = (%d, %d, %v), want (KindFinalityVote, 3, true)", kind, subnet, ok)
	}
	for _, topic := range []string{
		validator.BlockTopic,
		validator.AggregateTopic,
		validator.ColumnTopic(2),                  // a non-partial prefix family
		validator.AvailabilityVoteTopic,           // AC votes stay classic (locked)
		validator.AttestationTopicPrefix + "x/ssz_snappy", // malformed subnet
		"/unknown/topic",
	} {
		if _, _, ok := partialKindFor(topic); ok {
			t.Errorf("partialKindFor(%q) matched, want no match", topic)
		}
	}
}
