# plan.md — slot simulator: architecture & build order

A design plan only — concepts, seams, and sequencing. **No code.**

Build a **simnet-first** simulator — runs in-process, fast and local, easy to unit-test —
that **also runs on Shadow** (real binaries, scale) behind the *same* node logic. Start with
the minimal slot loop, then data columns, then gloas. See `slot-messages.md` (message set,
sizes, timing) and `scaling.md` (topology, custody, the sweep).

---

## 1. Goals

- **simnet-first, Shadow-capable.** The same behavior runs in-process (simnet) and as a Shadow
  node. Only the **network** swaps underneath; time is handled by the environment (see §2).
- **Modular code at the seams we'll actually vary — not a config system (yet).** First cut is
  clean seams *in code*; externalized configuration can come later. The two variations we
  intend to exercise:
  1. **Protocol profile** — the message set + its deadlines, so gloas/ePBS drops in as a variant.
  2. **Attestation behavior** — "who attests when" (on block-seen vs. at a fixed time).
  Everything else (N, V, distribution, sizes, the deadline values) is just set in code.
- **Incremental scope — one message at a time.** First just the **block** (one dissemination,
  measured). Then add **attestations**. Then **aggregations**. Then close the loop (next
  block). Then **data columns**. Later **gloas**.

---

## 2. Architecture — one seam

Everything above the seam is shared and backend-agnostic; below it, simnet and shadow are
interchangeable.

```
inputs (set in code): N, V, distribution, sizes, slot deadlines, message set, behavior policy
        │
   ┌────▼───────────────────────────────────────────┐
   │ SHARED (backend-agnostic)                         │
   │  • Behavior / roles      — pluggable policy        │  seam: behavior
   │  • Slot scheduler        — the deadlines            │  seam: profile
   │  • Message catalog       — type, topic, size, delay │  seam: profile
   │  • Validator / topology  — committees, subnets,      │  seam: topology
   │                            custody                  │
   │  • Metrics               — RPC tracer (send/recv)   │
   │  • gossipsub             — libp2p-pubsub             │
   └────┬─────────────────────────────────────┬─────────┘
        │           network transport          │  ← the seam
   ┌────▼─────────┐                     ┌──────▼────────┐
   │ simnet        │                     │ shadow         │
   │ in-process    │                     │ real host      │
   │ in-mem net    │                     │ Shadow net     │
   │ time: synctest│                     │ time: Shadow   │
   └──────────────┘                     └───────────────┘
```

**Time is not a component we build.** Node and gossipsub code use the standard `time` package —
a normal clock. In tests, `synctest` runs it in a bubble and controls that clock (instant
fast-forward, deterministic scheduling); in shadow, Shadow virtualizes the clock kernel-side;
in a plain run it's the wall clock. Same code — the environment handles time.

**The network is the only real seam:** an in-memory transport in simnet (no real sockets, so
synctest can idle and advance), real libp2p over Shadow's network in shadow. Gossipsub and
node logic sit above it, identical in both.

---

## 3. Flexibility seams — code modularity, not config (yet)

First cut: keep these as clean seams *in code* — changeable without surgery; a real config
surface can come later.

| Seam | What it is | A change is |
|------|-----------|-------------|
| Protocol profile | the message set + its deadlines (a fork's shape) | add a variant in code — gloas |
| Behavior | each node's policy for what to publish, and when | swap a policy in code — who attests when |
| Topology | validators→nodes mapping; committee, subnet, custody rules | swap a rule in code — custody / subnet / distribution |

We'll actively vary the first two early (gloas; who-attests-when); topology stays modular
because committee / subnet / custody rules shift across forks and experiments.

A **message** is a payload of a declared size, carrying **real bytes** (random filler sized to
the target — not an empty slice, which protobuf would drop). No crypto, no meaning.

**Gossip validation is mocked as a sleep.** Shadow and simnet don't bill CPU, so each message
type's verification cost is reproduced by a gossipsub topic validator that *sleeps* that long,
then accepts. Gossipsub won't forward a message until its validator returns, so the sleep sits
on the propagation path at **every hop** — which is what makes dissemination timing match
mainnet. Reuse batched-attestation-sim's batched verifier (batch window + base delay + per-item
cost, modeling batched signature verification). Under synctest the sleep is virtual (instant,
deterministic); under Shadow it's real time. *(This is the per-hop "processing delay" of
`scaling.md §7`; the batch is its queue.)*

A **behavior** is separate — a node's reaction policy: on slot tick / received message /
deadline, decide what to publish ("process the block, then attest at the deadline" vs "attest
at a fixed time"). That's the behavior seam.

Varying slot timing as a *swept* knob is a measurement-phase concern (§6 Phase 4), not a config
surface to build now.

---

## 4. simnet vs shadow

| | simnet | shadow |
|---|--------|--------|
| Nodes | many, one process | N real processes |
| Time | standard `time`, faked by **synctest** in tests | standard `time`, faked by Shadow |
| Network | in-memory, set latency/bw | real libp2p over Shadow |
| For | dev, unit tests, structure, quick runs | fidelity, scale, **the actual measurements** |
| Launch | one process runs all nodes | one node program; Shadow launches N |

Determinism in simnet = synctest (controls time + scheduling) + seeded randomness (aggregator
selection, latency draws, topology) → reproducible runs. simnet is for *structure and
behavior*; shadow is the source of truth for *numbers*.

---

## 5. Key decisions

- **Gossipsub — real libp2p-pubsub on both backends**, parameters taken from **Prysm**
  (`../prysm` `beacon-chain/p2p/pubsub.go: pubsubGossipParam()` — D/Dlo/Dhi, 700 ms heartbeat,
  mcache length/gossip), as batched-attestation-sim already mirrors. Same pubsub + same params
  in-process (simnet) and over Shadow → mesh behavior matches mainnet.
- **Metrics — reuse batched-attestation-sim's RPC tracer** (`pubsub.RPCTracer`): it logs every
  message send/receive (stable per-message seq → arrival times + hop reconstruction), the
  control gossip (IHAVE/IWANT/GRAFT/PRUNE bytes), and mesh size/RTT — exactly the
  arrival / hop / byte records `scaling.md §9` calls for.
- **Time — `synctest`, not a clock we build.** All simnet tests run in a synctest bubble: node +
  gossipsub code uses the standard `time` package and synctest fakes it (every goroutine in the
  bubble, gossipsub's heartbeat included) → fast + deterministic, with real gossipsub. The one
  requirement is that simnet does no real I/O — in-memory transport only — so the bubble can
  idle and advance. *Risk to clear at the gossipsub step:* libp2p's background goroutines must
  all park inside the bubble (no real sockets, no busy-poll), or synctest can't advance.
- **Stack — Go + go-libp2p** (matches the existing Shadow harnesses; synctest needs Go 1.25).
  Reuse **`simctl` from batched-attestation-sim** for orchestration / Shadow runs, and its node
  + batched-verifier patterns as the starting point.

---

## 6. Build order

**Phase 1 — minimal slot loop (simnet).** *The first thing.* The loop isn't the bottom — a
single broadcast is. A slot is just "publish the right blob at the right time" on top of it.
Time is free (synctest), so there's nothing to build before the network.

*Engine — each tiny, run in a synctest bubble, unit-tested before the next:*
1. Two nodes over the in-mem transport: one opaque blob A→B, arrival recorded. *(network +
   metrics; also the first proof synctest advances our setup)*
2. N nodes + gossipsub: one node broadcasts one blob; every node records arrival + hop count.
   *(the synctest + real-gossipsub integration proof — clears the libp2p-parks-in-bubble risk;
   already a useful dissemination sim on its own)*

*Slot loop — add one message type at a time on top of #2:*
3. Proposer publishes a "block" blob (configured size) at t=0.
4. Each node attests — a blob to its committee subnet — at the attestation deadline.
5. Aggregators collect their subnet and publish an "aggregate" at the aggregate deadline.
6. Rotate the proposer; run K slots — the loop closes.

Behavior policy for step 4: "attest when the block is seen, else at the deadline." Topology:
committees + the 2-subnet backbone + aggregator selection.
- Tests: each step deterministic in the synctest bubble — deadlines fire, attest-on-block vs
  attest-at-deadline, aggregate count matches the committee count.
- **De-risk early:** a one-node-in-Shadow smoke test (real host, Shadow time) to prove the
  network seam before building far on it.

**Phase 2 — Data columns (simnet).**
- 128 column subnets; custody that scales with stake (per `scaling.md §1`); the distribution
  knob (uniform ↔ skew); t=0 publish + download; sampling as request/response.
- Tests: custody assignment; per-node column load under uniform vs skew.

**Phase 3 — Shadow backend + parity.**
- The real-host side of the seam (Shadow handles time); Shadow setup for N nodes.
- Parity: the same scenario on simnet and shadow at small N agrees within tolerance.
- *(Could swap with Phase 2 if you'd rather fully prove the abstraction on the simple loop
  first — walking skeleton. The Phase-1 smoke test is the minimum either way.)*

**Phase 4 — Scale-out + measurement.**
- The sweep + per-hop / CDF tooling + extrapolation (per `scaling.md`): record arrival / hops /
  bytes (via the RPC tracer); fit the arrival curve against depth; climb N to find the Shadow
  ceiling.

**Phase 5 — Gloas / ePBS.**
- Add the bid / payload-envelope / PTC message types + the GLOAS deadlines. A pure exercise of
  seams 1 and 2.

---

## 7. Open questions

- Which existing harness to reuse as the shadow base?
- Everything in §5 is settled; the one thing to verify empirically is that libp2p parks
  cleanly inside the synctest bubble — that's engine step 2, not a design question.
