# n1000 — 1 MiB block dissemination on ethp2p

Mainnet-scale block dissemination on `sukun@ethp2p`: **1000 nodes, 1 MiB blocks,
20% super nodes, 5 slots**. Config: `configs/n1000_1mb.yaml`.

## Latest result — full dissemination ✅
Every non-proposer received every block.

```
nodes: 1000  blocks published: 5
arrivals: 4995 (expected 4995)   missing: 0  duplicates: 0
arrival-delay CDF (ms): p50=3830  p90=5369  p99=6047  p100=6520
```

p100 (6.5 s) sits well inside the 12 s slot; the 30 s post-run drain caught the
whole tail (0 missing).

## What changed to make this work
- **gossipsub max message size → 10 MiB** (`node/node.go`, = mainnet
  `GOSSIP_MAX_SIZE`). The library default is 1 MiB, which silently drops a 1 MiB
  block once wrapped in its protobuf/pubsub envelope. The first run got **0/4995**;
  reproduced + guarded by `TestLargeBlockDisseminates` (5-node simnet test).
- **Bring-up staggered like batched-attestation-sim** (`cmd/slot-sim-node/main.go`):
  each host dials peers, waits a random 0-30 s, joins the topic, then settles to
  slot 0 (`startup_seconds: 150`). Post-run drain widened to 30 s for the 1 MiB tail.

## Re-run on the remote
```bash
uv run simctl run --config configs/n1000_1mb.yaml --remote sukun@ethp2p \
  --output-dir runs/n1000-1mb
```
rsyncs the repo, builds + runs Shadow on the remote, tarballs the output dir to
`~/eth-slot-sim/runs/n1000-1mb.tar.gz`, then removes the dir. (The local side only
monitors — the Shadow run is `nohup`'d on the remote and survives a laptop shutdown.)

## Fetch + analyze later
```bash
scp sukun@ethp2p:~/eth-slot-sim/runs/n1000-1mb.tar.gz /tmp/n1000.tar.gz
mkdir -p /tmp/n1000 && tar -xzf /tmp/n1000.tar.gz -C /tmp/n1000
uv run python analysis/check_arrivals.py /tmp/n1000/n1000-1mb/run-*
```

Results tarball on remote: `~/eth-slot-sim/runs/n1000-1mb.tar.gz`
(run dir inside: `n1000-1mb/run-<timestamp>-n1000-D8/`).
