package node

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/slot-sim/pb"
	"github.com/ethp2p/slot-sim/validator"
)

// Identity is a message's metrics-identity tuple — the (kind-less) MsgID the tracer keys
// publish/receive joins and the CSV on. The per-kind encoding policy lives in the registry
// below, in ONE place: kinds without a meaningful field carry -1, and kinds whose publisher
// IS the message identity (aggregate-likes) carry it in Attester with Origin -1, because the
// CSV has no origin column. Received.Origin separately carries the publishing node for the
// loopback skip, which Identity.Origin then need not.
type Identity struct {
	Slot, Subnet, Attester, Origin int
}

// verifyClass names the validation-as-sleep queue a topic's messages route through (§8.1):
// the block's fixed per-hop delay, the width-P column verifier, or a batched single-server
// flood queue ("one CPU per flood"). Batch classes map 1:1 onto Node.verifiers keys: the
// present-day t≈4s/t≈8s floods share "consensus" (as today), while the decoupled floods each
// get their own queue so they do not contend with each other.
type verifyClass string

const (
	vcFixed     verifyClass = "fixed"     // fixed per-hop VerifyDelay sleep (the block)
	vcColumn    verifyClass = "column"    // width-P column verifier (the bursty t=0 wave)
	vcConsensus verifyClass = "consensus" // attestations + aggregates + sync msgs/contributions
	vcAC        verifyClass = "ac"        // availability votes
	vcFCVote    verifyClass = "fcvote"    // finality votes
	vcFCAgg     verifyClass = "fcagg"     // finality aggregates
)

// descriptor is one message kind's registry entry: how its topic is matched, which verify
// queue it routes through, and how its payload decodes into (object, identity, publisher).
// Adding a message type = a proto + a validator.Make* + one entry here.
type descriptor struct {
	kind   Kind
	topic  string      // exact topic — exactly one of topic/prefix is set
	prefix string      // topic prefix (per-subnet topic families)
	class  verifyClass // never "" (validated at init)
	decode func(data []byte) (obj any, id Identity, origin int, err error)
}

// decodeAs builds a descriptor's decode func from one per-kind identity extractor. The
// publisher origin is pulled generically (every pb message carries Origin), so an entry
// cannot get the loopback origin wrong; only the identity policy is per-kind.
func decodeAs[T any, PT interface {
	*T
	proto.Message
	GetOrigin() uint32
}](extract func(PT) Identity) func([]byte) (any, Identity, int, error) {
	return func(data []byte) (any, Identity, int, error) {
		m := PT(new(T))
		if err := proto.Unmarshal(data, m); err != nil {
			return nil, Identity{}, 0, err
		}
		return m, extract(m), int(m.GetOrigin()), nil
	}
}

// registry is the per-kind message table. Topic lookup is exact-match first, then prefix
// scan (see descriptorFor), so an exact topic sharing a stem with a prefix family — the
// sync contribution topic literally starts with the sync message prefix — resolves by rule,
// not by entry order. Identity policies mirror the metrics.*ID constructors (the publish
// side); metrics/roundtrip_test.go pins the two against each other.
var registry = []descriptor{
	{kind: KindBlock, topic: validator.BlockTopic, class: vcFixed,
		decode: decodeAs(func(b *pb.Block) Identity {
			return Identity{int(b.Slot), -1, -1, int(b.Origin)}
		})},
	{kind: KindAttestation, prefix: validator.AttestationTopicPrefix, class: vcConsensus,
		decode: decodeAs(func(a *pb.Attestation) Identity {
			return Identity{int(a.Slot), int(a.Subnet), int(a.Val), int(a.Origin)}
		})},
	// One distinct aggregate per aggregator: the aggregator node is the identity, carried in
	// Attester (the CSV has no origin column), Origin -1 — exactly as AggregateID encodes it.
	{kind: KindAggregate, topic: validator.AggregateTopic, class: vcConsensus,
		decode: decodeAs(func(a *pb.Aggregate) Identity {
			return Identity{int(a.Slot), int(a.Subnet), int(a.Origin), -1}
		})},
	// The column index rides Subnet (one sidecar per column subnet); Attester is unused.
	{kind: KindColumn, prefix: validator.ColumnTopicPrefix, class: vcColumn,
		decode: decodeAs(func(c *pb.Column) Identity {
			return Identity{int(c.Slot), int(c.Column), -1, int(c.Origin)}
		})},
	// One message per member: the member node is the identity, in Attester (like aggregates).
	{kind: KindSyncMessage, prefix: validator.SyncMessageTopicPrefix, class: vcConsensus,
		decode: decodeAs(func(m *pb.SyncMessage) Identity {
			return Identity{int(m.Slot), int(m.Subnet), int(m.Origin), -1}
		})},
	{kind: KindSyncContribution, topic: validator.SyncContributionTopic, class: vcConsensus,
		decode: decodeAs(func(c *pb.SyncContribution) Identity {
			return Identity{int(c.Slot), int(c.Subnet), int(c.Origin), -1}
		})},
	// No subnets on the availability chain: Subnet -1, the voting validator in Attester.
	{kind: KindACVote, topic: validator.AvailabilityVoteTopic, class: vcAC,
		decode: decodeAs(func(v *pb.ACVote) Identity {
			return Identity{int(v.Slot), -1, int(v.Val), int(v.Origin)}
		})},
	// The finality slot rides Slot for both finality kinds.
	{kind: KindFinalityVote, prefix: validator.FinalityVoteTopicPrefix, class: vcFCVote,
		decode: decodeAs(func(v *pb.FinalityVote) Identity {
			return Identity{int(v.FinalitySlot), int(v.Subnet), int(v.Val), int(v.Origin)}
		})},
	{kind: KindFinalityAggregate, topic: validator.FinalityAggregateTopic, class: vcFCAgg,
		decode: decodeAs(func(a *pb.FinalityAggregate) Identity {
			return Identity{int(a.FinalitySlot), int(a.Subnet), int(a.Origin), -1}
		})},
}

var exactTopics, prefixTopics = buildLookups(registry)

// buildLookups indexes the registry for descriptorFor and rejects malformed or ambiguous
// tables at init: duplicate kinds, duplicate exact topics, a prefix that prefixes another
// prefix (which would make prefix-scan order matter), both/neither of topic+prefix, or an
// unset class (the zero value must not silently mean "fixed delay").
func buildLookups(regs []descriptor) (map[string]*descriptor, []*descriptor) {
	exact := make(map[string]*descriptor, len(regs))
	var prefixes []*descriptor
	kinds := make(map[Kind]bool, len(regs))
	for i := range regs {
		d := &regs[i]
		if kinds[d.kind] {
			panic(fmt.Sprintf("registry: duplicate kind %d", d.kind))
		}
		kinds[d.kind] = true
		if d.class == "" {
			panic(fmt.Sprintf("registry: kind %d has no verify class", d.kind))
		}
		if (d.topic == "") == (d.prefix == "") {
			panic(fmt.Sprintf("registry: kind %d must set exactly one of topic/prefix", d.kind))
		}
		if d.topic != "" {
			if _, dup := exact[d.topic]; dup {
				panic(fmt.Sprintf("registry: duplicate topic %q", d.topic))
			}
			exact[d.topic] = d
			continue
		}
		for _, p := range prefixes {
			if strings.HasPrefix(d.prefix, p.prefix) || strings.HasPrefix(p.prefix, d.prefix) {
				panic(fmt.Sprintf("registry: ambiguous prefixes %q and %q", d.prefix, p.prefix))
			}
		}
		prefixes = append(prefixes, d)
	}
	return exact, prefixes
}

// descriptorFor resolves a topic to its registry entry: exact match first, then prefix scan
// (order-independent — buildLookups rejects overlapping prefixes). Nil for an unknown topic.
func descriptorFor(topic string) *descriptor {
	if d, ok := exactTopics[topic]; ok {
		return d
	}
	for _, d := range prefixTopics {
		if strings.HasPrefix(topic, d.prefix) {
			return d
		}
	}
	return nil
}

// verifyClassFor reports the verify queue topic routes through. An unregistered topic falls
// back to the fixed per-hop delay (reachable only if a caller joins a topic outside the
// registry; every simulated topic has an entry).
func verifyClassFor(topic string) verifyClass {
	if d := descriptorFor(topic); d != nil {
		return d.class
	}
	return vcFixed
}
