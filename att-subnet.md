# Att subnets — dynamic subnet membership (discv5-simulated)

Replaces the static subnet-edge overlay (`augment_subnet_edges` / `subnetAwareGraph`)
with a stable subscriber set + per-slot publisher dials, peer count bounded ~70.
Aggregation is out of scope for now (no per-slot aggregator role).

The committee *draw* (which validators attest which subnet each slot) is unchanged; this
is only about subnet **membership** (who receives/relays) and **connectivity**.

## 1. Subscribe set (stable membership)
- For each **active** subnet (the `C` used per slot, not all 64): assign a floor of ~10
  nodes, then assign the remaining nodes randomly across the active subnets. So every
  active subnet has ≥10 subscribers; target ~2 subnets/node.
- Seeded; identical on both backends. Stable for the whole run.
- Every node knows the full map: node *i* can look up "subnet 3 = {a,b,c,…}".
- Carried in `committee.json` as `subnet_subscribers` (subnet → members); passed to Shadow
  hosts via `--att-subnet-subscribe` (simnet reads the same file).

## 2. Topology — simulate discv5 (the 70)
- Every node has ~70 long-lived peers, **discv5-biased**: for each subnet a node
  subscribes, reserve ~`D` (8) of the 70 for peers also on that subnet (subnet-mates), so
  the subnet's subscribers mesh out of the 70. Fill the rest up to ~70 with random peers.
- The block topic needs no special care: a connection carries every topic, so subnet-mate
  links relay the block too, and ~70-degree keeps the graph connected + low-diameter
  regardless of bias. The random fill is just top-up to reach 70 when a node has fewer than
  ~70 subnet-mates (small N / small subnets); at scale it shrinks toward zero.
- Built once, seeded, shared by both backends (replaces the static overlay).

## 3. Publish — per-slot dial / disconnect
- **Slot start:** every node dials **2 random members** of each subnet it will attest on
  this slot — the subnets of *all* its validators' committees. Fan-out publish, just the
  connection (no GRAFT). The 2 land on meshed subscribers, which relay to the rest.
- **Emit:** attestations on those subnets at `min(block_processed, deadline)` (unchanged).
- **Slot end:** disconnect the peers dialed at slot start, iff dialed *and* not already in
  the 70.

## 4. What changes
- **delete** `augment_subnet_edges` (topology.py) + `subnetAwareGraph` (netsim) — reach now
  comes from the discv5-biased 70 + the per-slot dial.
- **committee.py / committee (Go):** replace node-id backbone with the subscribe set
  (≥10/active subnet, ~2/node); drop aggregators. `subscribers(subnet)` is now stable
  (slot-independent). Committee draw unchanged.
- **node:** add `Disconnect(peers)`.
- **driver/NodeRunner:** subscribe the node's subscribed subnets at bring-up (stable mesh);
  per slot dial 2 members/duty-subnet, disconnect the non-70 ones at slot end; no per-slot
  aggregator subscribe.
- **topology.py / netsim:** build the discv5-biased 70 from the subscribe map.
- **check_arrivals:** `subscribers(subnet)` = the subscribe set; expected/missing/leaked as
  today.

## 5. Invariants
- Arrival set is fixed by the stable, seeded subscribe membership ⇒ deterministic across
  backends (only delays vary). Per-subnet coverage = every subscriber receives; no-leak =
  no non-subscriber receives. Peer count stays ~70 + a transient handful per slot.
