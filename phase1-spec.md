# Phase 1 — Block dissemination (simnet)

Draft spec. Implements `plan.md` §6 Phase 1, narrowed to **just the block**: each slot one node
(cyclic) publishes one opaque, correctly-sized **block** on a global gossipsub topic; every
other node receives and relays it; we record per-node **block-arrival time**. Pure Go,
in-process **simnet** — no Shadow, no Python `simctl`.

The deliverable is block dissemination **plus** a clean split — **Node** (networking + timing)
vs. **Validator** (duties + message creation) — so adding attestations / columns later grows
the validator, not the node. We reuse the mechanical gossipsub/simnet/synctest **scaffolding**
from `batched-attestation-sim`, but **not** its attestation-specific node code.

> Sections tagged **(confirm Qn)** depend on an open question in §11.

---

## 0. Non-goals (Phase 1)

Attestations, aggregates, sync, data columns, req/resp sampling, hop-depth/relay-tree metrics,
realistic topology, Shadow backend, `simctl`/Python orchestration, crypto, real SSZ, mainnet
calibration. The Node/Validator split is *built so these slot in*; only the block is *built now*.

---

## 1. Reuse vs. write fresh

**Reuse — mechanical, message-agnostic (copy/adapt verbatim values):**

- GossipSub parameter block tuned to Prysm (`plan.md` §5): `D=8, Dlo=6, Dhi=12, Dlazy=6,
  Dout=1`, heartbeat `700ms`, history `6/3`, `FanoutTTL 60s`; `StrictNoSign` + `NoAuthor`;
  `MessageIdFn = sha256(topic ‖ len ‖ data)[:20]`; flood-publish + **IDONTWANT** on
  (`scaling.md` §6) — but see §6 for how flood-publish interacts with connectivity.
- simnet wiring: `marcopolo/simnet` + `go-libp2p/x/simlibp2p.QUICSimnet`, `IntToPublicIPv4(i)`,
  deterministic per-node Ed25519 keys (`seed = node num`) → peer IDs resolvable by number with
  no discovery layer.
- The **Network seam** — `PeerAddr(nodeNum) → multiaddr` — simnet impl now, Shadow (DNS) later.
- The synctest harness pattern. (`batched-attestation-sim`'s tests already run real gossipsub
  inside `synctest.Test`, so the *"does libp2p park cleanly in the bubble"* risk from `plan.md`
  §7 is **already cleared**.)

**Write fresh:**

- A **message-agnostic Node** (networking + slot timing) split from a **Validator** (duties +
  message creation) — §3.
- The **simnet module** (`netsim`): builds hosts with **random per-link latency** + **per-node
  bandwidth classes**, hands each node a placeholder **peer list** (§6). *You flagged we'll need
  to create this.*
- A protobuf wire **message** carrying sized random filler — §4.
- **Metrics**: block-arrival time, in-process CDF — §8.

---

## 2. Repo & module layout (confirm Q5)

```
go.mod                  module <path> (proposed github.com/ethp2p/slot-sim), go 1.25+
cmd/slot-sim/main.go    wall-clock simnet driver: build scenario → run → print CDF
node/                   Node: host, gossipsub, topics, peers, slot loop, send/recv, metrics hookup
validator/              Validator + Duty + Message + block creation (makeBlock)
netsim/                 simnet module: hosts, random latency, bandwidth classes, Network seam
metrics/                Tracer, arrival records, CDF summary
pb/                     protobuf-generated message type(s) (Q6 = protobuf)
```

`go 1.25+` for `testing/synctest`. **No topology-generation code in Go** — the node consumes a
peer list it's handed; the *realistic* graph comes from Python later (§6).

---

## 3. Design — Node + Validator

Two concrete types, one clean split. No interface zoo.

- **Node** = network + timing. Owns the libp2p host, gossipsub, topic joins, peer connections,
  the slot clock, send/receive, metrics. Knows nothing about "block." Each slot it **asks its
  validator for duties** and sends them.
- **Validator** = the duty + message producer a node runs. Given a slot, returns the duties
  (message + when-in-the-slot to send it). Block creation lives here. Knows nothing about
  networking.

```go
// validator package
type Message struct {            // the wire payload, already marshaled
    Topic   string               // /eth2/<digest>/beacon_block/ssz_snappy
    Payload []byte               // protobuf Envelope: header + random filler sized to target (§4)
    Slot    int                  // metrics correlation (also inside the Envelope)
}

type Duty struct {
    Msg Message
    At  time.Duration            // when to publish, as an offset into the slot
}

type Validator struct {
    self      int                // this node's index
    n         int                // total nodes
    blockSize int
    offset    time.Duration      // base lead into the slot
    jitter    time.Duration      // K — publish at offset + rand(0,K)
    rng       *rand.Rand
}

// Duties for this slot. Phase 1: propose a block iff it's our turn (cyclic).
func (v *Validator) Duties(slot int) []Duty {
    if slot%v.n != v.self {
        return nil
    }
    at := v.offset + time.Duration(v.rng.Int64N(int64(v.jitter)))
    return []Duty{{Msg: makeBlock(slot, v.self, v.blockSize), At: at}}
}
```

```go
// node package — the loop never mentions "block"
for slot := 0; slot < numSlots; slot++ {
    slotStart := n.runStart.Add(time.Duration(slot) * n.slotDuration)
    for _, d := range n.validator.Duties(slot) {
        when := slotStart.Add(d.At)
        time.AfterFunc(time.Until(when), func() { n.publish(d.Msg) }) // Topic + Payload onto gossipsub
    }
    time.Sleep(time.Until(slotStart.Add(n.slotDuration)))
}
// receive goroutines decode each Envelope and record arrival via the Tracer
```

A node runs one validator in Phase 1 (`self` becomes a slice to match `V/N` later). Adding
attestations later = the validator gains an attest duty + `makeAttestation`. **The node's
send/receive loop never changes** — the "in between" you asked for: one real split, no
catalog/behavior/topology interfaces.

**Naming note.** Our `Validator` is the *duty actor*. The gossipsub *topic validator* that
models processing delay by sleeping (§5) is a different thing — this spec calls that the
**verify hook**, never "validator."

---

## 4. Wire format — protobuf (Q6 = protobuf)

A block is opaque bytes of a target size carrying **real random filler** (not zeros — protobuf
drops them; `plan.md` §3). One generic Envelope all future message types reuse:

```proto
message Envelope {
  uint32 type    = 1;   // 0 = beacon_block (room for attestation/column/… later)
  uint32 slot    = 2;
  uint32 origin  = 3;   // publishing node number
  bytes  payload = 4;   // random filler, sized so the marshaled Envelope ≈ target bytes
}
```

`makeBlock` fills `payload` so the marshaled size hits `blockSize`. **Snappy: off** — random
filler is incompressible, so post-snappy size ≈ payload size; we publish raw bytes and keep
`ssz_snappy` in the topic name only. Topic: `/eth2/<fork-digest>/beacon_block/ssz_snappy`
(placeholder digest).

---

## 5. Processing delay — the verify hook (validation-as-sleep)

`plan.md` §3 / `scaling.md` §7: no crypto; each topic registers a gossipsub validator callback
(the **verify hook**) that **sleeps** the message's processing cost, then accepts — gossipsub
won't forward until it returns, so the sleep sits on the propagation path **at every hop**.

- **Block (Phase 1):** a **fixed** delay (one block/slot — no batching). Placeholder `10ms`,
  calibrate later (`scaling.md` §8). **(confirm Q8)**
- **Later:** the hook can express `fixed + perItem·n` + a batch window so the batched
  verifier slots in for attestations without touching the block path.

---

## 6. Connectivity & the simnet module (`netsim`) — (confirm Q7)

**No topology code in Go.** The node is *handed* a peer list (`ConnectToPeers([]int)`); in the
long run Python generates the realistic graph and passes it in (as the existing harness does via
a `-peer-nums` flag). For Phase 1, `netsim` hands each node a **throwaway placeholder** peer set
so gossip has somewhere to flow.

**The flood-publish catch (decision needed).** Prysm runs flood-publish: a publisher sends to
*all* topic peers it's connected to, not just its mesh. So the placeholder connectivity decides
whether we even have a dissemination curve:

- **Full mesh** (every node ↔ every node) + flood-publish ⇒ block reaches everyone in **1 hop**.
  No curve. Also O(N²) connections — won't scale past small N.
- **Bounded random peers** (each node ↔ ~`P` random others) + flood-publish ⇒ realistic
  multi-hop spread, scales. **Recommended:** `P` ≈ a few tens (placeholder, not a topology
  model — just "give gossip a peer set"; Python replaces it).

So Phase 1 default: **bounded random `P` peers per node, flood-publish on.** (Confirm `P`, or
prefer full-mesh-with-flood-publish-off.)

**The simnet module provides:**
- **Random per-link latency** — a `LatencyFunc(a,b) → duration`, random per node-pair (seeded).
  Range is a placeholder (Q10).
- **Per-node bandwidth classes** (Q4 answer):
  - **regular:** 25 Mbps up / 50 Mbps down
  - **supernode:** 1024 Gbps up/down (effectively unlimited)
  - Supernode share is a knob — default small (**confirm Q12**).
- Deterministic keys + `PeerAddr` (the Network seam).

`V`/committees/custody are irrelevant to a block (`scaling.md` §2: block spread is the clean,
`V`-independent metric). Phase 1 ignores `V`.

---

## 7. Run model & time

- **Run for `X` seconds**, slot duration `S` seconds ⇒ `numSlots = X / S`.
- **Cyclic proposer:** slot `k` is proposed by node `k mod N` (node 0 slot 0, node 1 slot 1, …).
  Run `X ≥ N·S` to let every node propose at least once.
- **Publish time:** the proposer publishes at `slotStart(k) + offset + rand(0,K)` — the jitter
  spreads proposal times into a realistic distribution (`slot-messages.md` §4.1 relay offset).
- Same loop under two clocks:
  - **`cmd/slot-sim` binary** — wall-clock simnet, real time. For runs by hand (smaller `N`).
  - **`node/` tests** — `testing/synctest`, virtual clock: the whole `X`-second run is instant
    and deterministic, with real gossipsub. Where exact, repeatable numbers come from.

Defaults for `X, S, offset, K` in §10 (confirm Q13).

---

## 8. Metrics & output

Per `scaling.md` §9, Phase-1 subset — **arrival time only** (hop depth deferred, Q4 answer):

- **Block-arrival time** per node per slot: `recv_time − publish_time` (and vs. slot start).
- **Tracer** is the one small interface the Node calls on publish/receive, so tests collect
  in-memory while the binary writes CSV. Not a message abstraction — just an output sink.
- **Output:** print `p50/p90/p99/p100` + a CDF dump (CSV/JSON). `p99–p100` is the number that
  matters (`scaling.md` §9). **(confirm Q9.)**

---

## 9. Build / test milestones (each a synctest unit test before the next)

1. **2 nodes, gossipsub, one block A→B**, arrival recorded. Smallest end-to-end.
2. **`netsim` module:** N hosts, random latency, bandwidth classes, placeholder peer lists.
3. **N nodes, one global topic, cyclic proposer, `X`-second run**; every node records each
   block's arrival. Assert: every non-proposer receives each block exactly once; arrival spread
   looks multi-hop (not all ~1 hop) — the flood-publish/connectivity check from §6.
4. **Driver** (`cmd/slot-sim`): wall-clock run, prints the arrival CDF.

---

## 10. Proposed defaults (all in code, no config system yet — `plan.md` §3)

| Knob | Proposed default | Notes / source |
|------|------------------|----------------|
| `N` (nodes) | tests 100; binary 25 | sweep is Phase 4 |
| `S` slot duration | 12 s | mainnet; the knob this sim exists to vary |
| `X` run duration | `N·S` | so every node proposes once (Q13) |
| `offset` / `K` jitter | 0 / 2 s | proposer publishes at `offset+rand(K)` (Q13) |
| Block size | 128 KiB | within `slot-messages.md` "typ 100–300 KB" |
| Verify-hook delay | fixed 10 ms | placeholder; calibrate later (Q8) |
| Per-link latency | random, uniform 10–150 ms one-way | placeholder (Q10) |
| Bandwidth — regular | 25↑ / 50↓ Mbps | your answer |
| Bandwidth — supernode | 1024 Gbps | your answer; share TBD (Q12) |
| Peers per node `P` | ~20 random | placeholder connectivity (Q7) |
| GossipSub `D/Dlo/Dhi` | 8 / 6 / 12 | Prysm |
| Snappy | off | random filler ⇒ incompressible |

---

## 11. Decisions (defaults locked — override any anytime)

**Agreed with you:** Node + Validator split (§3); both binary + synctest tests (§7); cyclic
proposer, run for `X`s, publish at `offset+rand(K)` (§7); arrival-only metrics + two bandwidth
classes (§6, §8); protobuf wire (§4).

**Defaulted (all in §10 — say the word to change):**
- Module path `github.com/ethp2p/slot-sim`; layout §2.
- **Connectivity (the one that matters):** bounded `P≈20` random peers + flood-publish **on**,
  vs. full-mesh + flood-publish off. Defaulted to **bounded peers** (§6).
- Verify-hook `10 ms`; latency uniform `10–150 ms` one-way; supernode share `5%`.
- `S=12 s`, `X=N·S`, `offset=0`, `K=2 s`, block `128 KiB`, `N=100` (tests) / `25` (binary).
- Output: in-process CDF → CSV/JSON + printed `p50/90/99/100`.

---

## 12. Summary

A message-agnostic **Node** (gossip + slot timing) asks its **Validator** for duties each slot
and sends them; in Phase 1 the only duty is proposing a global **block**, cyclically, once per
node, over an `X`-second run. A new **`netsim`** module gives random per-link latency and two
bandwidth classes and hands nodes a placeholder peer list; real topology stays in Python. Output:
the per-node block-arrival CDF over `N` simnet nodes.
