# Att subnets — dynamic subnet membership (discv5-simulated)

Replaces the static subnet-edge overlay (`augment_subnet_edges` / `subnetAwareGraph`)
with a stable subscriber set + per-slot publisher dials, peer count bounded ~`K` (a config
knob, `topology.degree`; ~70–100 for mainnet). Aggregation is out of scope for now (no
per-slot aggregator role).

The committee *draw* (which validators attest which subnet each slot) is unchanged; this
is only about subnet **membership** (who receives/relays) and **connectivity**.

## 1. Subscribe set (stable membership)
- For each **active** subnet (the `C` used per slot, not all 64): every node is assigned
  `subnets_per_node` (default 2) random active subnets, then any subnet below
  `subscribe_floor` (~10) is topped up with random extra nodes. So every active subnet has
  ≥ `min(floor, N)` subscribers.
- Centrally generated and seeded; the node doesn't pick — it reads the map. Stable for the
  whole run, identical on both backends.
- Every node knows the full map: node *i* can look up "subnet 3 = {a,b,c,…}".
- Carried in `committee.json` as `subnet_subscribers` (subnet → members); passed to Shadow
  hosts via the `-committee=<path>` flag (simnet reads the same file via `SIMRUN_PARAMS`).

## 2. Topology — simulate discv5 (the K peers)
- Every node targets `K` long-lived peers, **discv5-biased** so each subnet's subscribers
  form a connected subgraph: take the subnet's assigned members and link them with random
  edges until there are no islands (a random spanning tree — each member attaches to a
  random earlier one). These subnet-mate links **count toward K**.
- A global random spanning tree over all N keeps the whole graph (and the block topic)
  connected; a connection carries every topic, so subnet-mate links relay the block too.
- Then **fill** each node with random peers until its total degree reaches K (not K more).
- **Soft target / graceful:** K is clamped to N−1; the fill is best-effort (bounded
  retries); a node already at K from subnet-mate links gets no fill (and may sit slightly
  above K); tiny/empty subnets are skipped. So small or dense graphs degrade, never crash.
- Built once, seeded. Both backends build the *same shape* from the subscribe map —
  `generate_subnet_topology` (simctl, the real runs) and `discv5Graph` (netsim, in-process
  Go tests) — satisfying the same invariants rather than being byte-identical (a
  cross-backend run shares one `topology.json`).

## 3. Publish — per-slot dial / disconnect
- **Slot start:** every node dials **2 random members** of each subnet it will attest on
  this slot — the subnets of *all* its validators' committees. Fan-out publish, just the
  connection (no GRAFT). The 2 land on connected subscribers, which relay to the rest.
- **Emit:** attestations on those subnets at `min(block_processed, deadline)` (unchanged).
- **Slot end:** disconnect the peers dialed at slot start, iff dialed *and* not already in
  the K long-lived peers.

## 4. What changes
- **topology:** `augment_subnet_edges` (topology.py) is replaced by `generate_subnet_topology`
  and `subnetAwareGraph` (netsim) by `discv5Graph` — reach comes from the discv5-biased K
  peers + the per-slot dial; both build the global tree + per-subnet trees + fill to K.
- **committee.py / committee (Go):** node-id backbone replaced by the subscribe set
  (≥`floor`/active subnet, `subnets_per_node`/node); aggregators dropped. `subscribers(subnet)`
  is stable (slot-independent). Committee draw unchanged.
- **node:** add `Disconnect(peers)`; per-slot `Unsubscribe` removed (duty subnets are
  Join-only / publish-only now).
- **driver/NodeRunner:** subscribe the node's subscribed subnets at bring-up (stable mesh);
  per slot dial 2 members/duty-subnet, disconnect the non-K ones at slot end; no per-slot
  aggregator subscribe.
- **check_arrivals:** `subscribers(subnet)` = the subscribe set; expected/missing/leaked as
  today.

## 5. Invariants
- Arrival set is fixed by the stable, seeded subscribe membership ⇒ deterministic across
  backends (only delays vary). Per-subnet coverage = every subscriber receives; no-leak =
  no non-subscriber receives. Peer count stays ~K + a transient handful per slot.
