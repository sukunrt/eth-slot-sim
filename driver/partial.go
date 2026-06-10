package driver

import (
	"slices"
	"time"

	"github.com/ethp2p/slot-sim/node"
	"github.com/ethp2p/slot-sim/schedule"
	"github.com/ethp2p/slot-sim/validator"
)

// PartialParams switches the attestation-class floods (standard attestations + finality votes
// when decoupled) to the partial-message transport (partial-attestation-spec.md §9); nil ⇒
// classic. It is a transport, not a phase: it requires the attestation phase or decoupled
// consensus to be on. Zero fields take the spec defaults.
type PartialParams struct {
	PublishInterval        time.Duration // tick period; 0 ⇒ 20ms
	MaxPeersPerAttestation int           // per-position forward cap; 0 ⇒ 2·D
	MaxIWantPerPosition    int           // per-position request cap; 0 ⇒ 10
	AttestationDataSize    int           // shared attestation_data bytes; 0 ⇒ 128
	SignatureSize          int           // per-vote signature bytes; 0 ⇒ 96
	DisableMetadataGossip  bool          // no-gossip variant: mesh push only
}

// NewPartialResolver builds the schedule-backed identity resolver the partial transport needs
// (node.PartialResolver): committee widths and position → (validator, hosting node) for both
// partial kinds. Read-only over the assignment — one resolver serves every node of a run (the
// Shadow binary builds its own from the same schedule.json, so both backends agree).
func NewPartialResolver(a *schedule.Assignment) node.PartialResolver {
	return partialResolver{a}
}

type partialResolver struct{ a *schedule.Assignment }

// committee returns slot `group`'s committee on subnet, nil when out of range — the standard
// kind's position space (committee → subnet is the generator's identity map, but resolve via
// SubnetOf to stay robust to that convention).
func (r partialResolver) committee(subnet, group int) []schedule.AttesterRef {
	if group < 0 || group >= len(r.a.Slots) {
		return nil
	}
	sp := r.a.Slots[group]
	ci := slices.Index(sp.SubnetOf, subnet)
	if ci < 0 {
		return nil
	}
	return sp.Committees[ci]
}

func (r partialResolver) CommitteeSize(kind node.Kind, subnet, group int) int {
	switch kind {
	case node.KindAttestation:
		return len(r.committee(subnet, group))
	case node.KindFinalityVote:
		return r.a.FinalityCellSize(subnet, group)
	}
	return 0
}

func (r partialResolver) Identity(kind node.Kind, subnet, group, position int) (val, origin int) {
	switch kind {
	case node.KindAttestation:
		for _, ref := range r.committee(subnet, group) {
			if ref.Position == position {
				return ref.Val, ref.Node
			}
		}
	case node.KindFinalityVote:
		if v := r.a.FinalityValAt(subnet, group, position); v >= 0 {
			return v, r.a.HostOf(v)
		}
	}
	return -1, -1
}

// partialOpts maps the run params onto one node's transport options (defaults resolved in
// node/; the sizes are resolved here because the runner builds the payloads).
func partialOpts(pp *PartialParams, seed uint64, resolver node.PartialResolver) *node.PartialOpts {
	return &node.PartialOpts{
		PublishInterval:        pp.PublishInterval,
		MaxPeersPerAttestation: pp.MaxPeersPerAttestation,
		MaxIWantPerPosition:    pp.MaxIWantPerPosition,
		DisableMetadataGossip:  pp.DisableMetadataGossip,
		Seed:                   seed,
		Resolver:               resolver,
	}
}

// dataSize / sigSize resolve the payload-size knobs against the spec defaults.
func (pp *PartialParams) dataSize() int {
	if pp.AttestationDataSize > 0 {
		return pp.AttestationDataSize
	}
	return validator.PartialAttDataSize
}

func (pp *PartialParams) sigSize() int {
	if pp.SignatureSize > 0 {
		return pp.SignatureSize
	}
	return validator.PartialSignatureSize
}
