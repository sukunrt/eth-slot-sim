"""DuckDB-backed twin of check_arrivals.build_report, reading parquet instead of slog text.

check_arrivals.py is the REFERENCE implementation: it defines every invariant (per-kind
identity encodings, required-vs-allowed finality receivers, host exclusion) in plain
Python over per-event tuples. That is exact but slow — an n4000 run is ~10 GB of JSON and
~10⁸ arrival tuples. This module re-expresses each analyzer as SQL over the parquet event
tables written by to_parquet.py, holding everything in one DuckDB connection so no Python
tuple per arrival is ever built.

The contract: build_report(run_dir, parquet_dir) returns (and writes to analysis.json)
EXACTLY the dict check_arrivals.build_report produces for the same run — pinned by the
equivalence tests in tests/test_duck_report.py. Anything schedule-shaped (membership sets,
published-aggregate draws, receiver rules) is NOT re-derived here: it is imported from
check_arrivals and only the per-event heavy lifting moves to SQL. Extend both paths
together, and extend the equivalence test with them.
"""

import json
from pathlib import Path

import duckdb

from analysis import check_arrivals as ca

# The uniform event identity: publish and arrival log the same fields, so joining on all
# five is equivalent to each analyzer's per-kind reduced key (unused fields are -1 on both
# sides; metrics/roundtrip_test.go pins this).
_IDENT = "kind, slot, subnet, attester, origin"
_JOIN = (
    "p.kind = a.kind AND p.slot = a.slot AND p.subnet = a.subnet "
    "AND p.attester = a.attester AND p.origin = a.origin"
)


def _val(con: duckdb.DuckDBPyConnection, sql: str):
    return con.execute(sql).fetchone()[0]


def _delays_sql(kind: int, arrivals: str = "arrivals") -> str:
    """Arrival delays (ms) of one kind: arrivals joined to their publish on the full
    identity — the SQL twin of each analyzer's pubs-dict lookup."""
    return (
        f"SELECT a.slot AS slot, (a.t_ns - p.t_ns) / 1e6 AS delay_ms "
        f"FROM {arrivals} a JOIN publishes p ON {_JOIN} WHERE a.kind = {kind}"
    )


def _rank_exprs(grid: tuple[float, ...]) -> str:
    """Nearest-rank percentile columns (rank = max(ceil(p/100·n), 1)), matching
    check_arrivals.percentile / the Go metrics package exactly."""
    return ", ".join(
        f"max(CASE WHEN rn = greatest(CAST(ceil({g / 100!r} * n) AS BIGINT), 1) "
        f"THEN delay_ms END)"
        for g in grid
    )


def _delay_stats(con, delays_sql: str) -> tuple[dict, dict]:
    """(cdf, percentile_grid) of a delays relation, one pass. Empty → 0.0 everywhere,
    like percentile([])."""
    row = con.execute(
        f"WITH r AS (SELECT delay_ms, row_number() OVER (ORDER BY delay_ms) AS rn, "
        f"count(*) OVER () AS n FROM ({delays_sql})) "
        f"SELECT count(*), {_rank_exprs(ca.PERCENTILES)} FROM r"
    ).fetchone()
    grid = {f"p{g:g}": (v if v is not None else 0.0) for g, v in zip(ca.PERCENTILES, row[1:])}
    cdf = {"count": row[0], **{p: grid[p] for p in ("p50", "p90", "p99", "p100")}}
    return cdf, grid


def _per_slot_stats(con, delays_sql: str) -> dict[str, dict]:
    """Per-slot headline CDFs of a delays relation (delay_details' per_slot view)."""
    rows = con.execute(
        f"WITH r AS (SELECT slot, delay_ms, "
        f"row_number() OVER (PARTITION BY slot ORDER BY delay_ms) AS rn, "
        f"count(*) OVER (PARTITION BY slot) AS n FROM ({delays_sql})) "
        f"SELECT slot, max(n), {_rank_exprs((50, 90, 99, 100))} FROM r "
        f"GROUP BY slot ORDER BY slot"
    ).fetchall()
    return {
        str(s): {"count": n, "p50": p50, "p90": p90, "p99": p99, "p100": p100}
        for s, n, p50, p90, p99, p100 in rows
    }


def _drops(con, kind: int) -> list[tuple]:
    """Publisher-side drops: publishes of the kind nobody received (full identity keys)."""
    return con.execute(
        f"SELECT {_IDENT} FROM "
        f"(SELECT DISTINCT {_IDENT} FROM publishes WHERE kind = {kind}) p "
        f"ANTI JOIN (SELECT DISTINCT {_IDENT} FROM arrivals WHERE kind = {kind}) a "
        f"USING ({_IDENT}) ORDER BY 1, 2, 3, 4, 5"
    ).fetchall()


def _missing_stats(con, table: str) -> tuple[int, dict[str, int], list[tuple]]:
    """(count, top-20 missing-by-node, first-20 examples) of a missing temp table whose
    columns are already the kind's example-tuple layout with node first."""
    count = _val(con, f"SELECT count(*) FROM {table}")
    by_node = con.execute(
        f"SELECT node, count(*) AS c FROM {table} GROUP BY node ORDER BY c DESC, node LIMIT 20"
    ).fetchall()
    examples = con.execute(f"SELECT * FROM {table} ORDER BY ALL LIMIT 20").fetchall()
    return count, {str(n): c for n, c in by_node}, examples


def _insert(con, name: str, cols: str, rows: list[tuple]) -> None:
    """Create temp table name(cols) and bulk-insert rows (schedule-scale, not event-scale)."""
    con.execute(f"CREATE OR REPLACE TEMP TABLE {name} ({cols})")
    if rows:
        ph = ", ".join("?" for _ in rows[0])
        con.executemany(f"INSERT INTO {name} VALUES ({ph})", rows)


def _dup_count(con, key: str, kind: int, arrivals: str = "arrivals") -> int:
    """Distinct per-receiver keys seen more than once — each analyzer's duplicates list."""
    return _val(
        con,
        f"SELECT count(*) FROM (SELECT {key} FROM {arrivals} WHERE kind = {kind} "
        f"GROUP BY ALL HAVING count(*) > 1)",
    )


def _kind_report(
    con,
    label: str,
    plural: str,
    kind: int,
    *,
    published: int,
    arrivals: int,
    expected: int,
    missing_table: str,
    leaked: int,
    leaked_examples: list[tuple],
    duplicates: int,
    ok: bool,
    delays_sql: str,
    per_slot_sql: str | None = None,
    voted: tuple[str, float] | None = None,
) -> dict:
    """Print one kind's section and return its JSON summary — the SQL-fed twin of
    check_arrivals._kind_report (same dict shape, same stdout lines)."""
    drops = _drops(con, kind)
    n_missing, missing_by_node, missing_examples = _missing_stats(con, missing_table)
    cdf, grid = _delay_stats(con, delays_sql)
    per_slot = _per_slot_stats(con, per_slot_sql or delays_sql)
    rep = {
        "published": published,
        "arrivals": arrivals,
        "expected": expected,
        "missing": n_missing,
        "leaked": leaked,
        "duplicates": duplicates,
        "publisher_drops": len(drops),
        "ok": ok,
        "cdf_ms": cdf,
        "percentiles_ms": grid,
        "per_slot": per_slot,
        "missing_by_node": missing_by_node,
        "missing_examples": [list(m) for m in missing_examples],
        "leaked_examples": [list(m) for m in leaked_examples],
        "drop_examples": [list(d) for d in drops[:20]],
    }
    if voted is not None:
        rep[voted[0]] = voted[1]

    def fmt(c: dict[str, float]) -> str:
        return f"p50={c['p50']:.1f} p90={c['p90']:.1f} p99={c['p99']:.1f} p100={c['p100']:.1f}"

    print(f"{plural} published: {published}")
    print(f"{label} arrivals: {arrivals} (expected {expected})")
    print(
        f"  missing: {n_missing}  leaked: {leaked}  "
        f"duplicates: {duplicates}  publisher drops: {len(drops)}"
    )
    if cdf["count"]:
        print(f"  {label} CDF (ms): {fmt(cdf)}")
    if len(per_slot) > 1:
        print("  per-slot CDF (ms):")
        for slot, c in per_slot.items():
            print(f"    slot {slot}: n={c['count']} {fmt(c)}")
    if missing_by_node:
        top = "  ".join(f"node {n}: {c}" for n, c in list(missing_by_node.items())[:10])
        print(f"  top missing receivers: {top}")
    if drops:
        print(f"  publisher drops (kind,slot,subnet,attester,origin): {drops[:10]}")
    if leaked_examples:
        print(f"  leaked (first 10): {list(leaked_examples)[:10]}")
    if voted is not None:
        print(f"  {voted[0].replace('_', ' ')}: {voted[1]:.3f}")
    return rep


def _pub_count(con, kind: int) -> int:
    return _val(
        con,
        f"SELECT count(*) FROM "
        f"(SELECT DISTINCT {_IDENT} FROM publishes WHERE kind = {kind})",
    )


def _arr_count(con, kind: int) -> int:
    return _val(con, f"SELECT count(*) FROM arrivals WHERE kind = {kind}")


def _voted_fraction(con, kind: int) -> float:
    n, v = con.execute(
        f"SELECT count(*), coalesce(sum(CASE WHEN voted_block THEN 1 ELSE 0 END), 0) "
        f"FROM publishes WHERE kind = {kind}"
    ).fetchone()
    return v / n if n else 0.0


def _blocks(con, node_count: int, kind: int = ca.BLOCK_KIND,
            label: str = "block", plural: str = "blocks") -> dict:
    """analyze() in SQL, for any block-shaped kind (blocks, ePBS consensus blocks /
    execution payloads): every node != origin receives each published message once."""
    n_pubs = _pub_count(con, kind)
    arrivals = _arr_count(con, kind)
    expected = n_pubs * (node_count - 1)
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE miss_k{kind} AS
        SELECT n.node AS node, p.slot AS slot, p.origin AS origin
        FROM (SELECT DISTINCT slot, origin FROM publishes WHERE kind = {kind}) p
        CROSS JOIN nodes n
        WHERE n.node != p.origin AND NOT EXISTS (
            SELECT 1 FROM arrivals a WHERE a.kind = {kind}
            AND a.node = n.node AND a.slot = p.slot AND a.origin = p.origin)"""
    )
    dups = _dup_count(con, "node, slot, origin", kind)
    n_missing = _val(con, f"SELECT count(*) FROM miss_k{kind}")
    ok = not n_missing and not dups and arrivals == expected
    return _kind_report(
        con, label, plural, kind,
        published=n_pubs, arrivals=arrivals, expected=expected,
        missing_table=f"miss_k{kind}", leaked=0, leaked_examples=[],
        duplicates=dups, ok=ok, delays_sql=_delays_sql(kind),
    )


def _payload_lag_cdf(con) -> dict:
    """payload_lag in SQL: per (node, slot), first execution-payload arrival minus first
    consensus-block arrival, over the nodes that received both."""
    lag_sql = (
        f"WITH cb AS (SELECT node, slot, min(t_ns) AS t FROM arrivals "
        f"WHERE kind = {ca.CONSENSUS_BLOCK_KIND} GROUP BY ALL), "
        f"ep AS (SELECT node, slot, min(t_ns) AS t FROM arrivals "
        f"WHERE kind = {ca.EXECUTION_PAYLOAD_KIND} GROUP BY ALL) "
        f"SELECT (ep.t - cb.t) / 1e6 AS delay_ms FROM ep JOIN cb USING (node, slot)"
    )
    cdf, _grid = _delay_stats(con, lag_sql)
    return cdf


def _membership_kind(
    con,
    label: str,
    plural: str,
    kind: int,
    members: str,
    *,
    self_col: str,
    key_cols: str,
    miss_cols: str,
    leak_cols: str,
    voted: tuple[str, float] | None = None,
) -> tuple[dict, bool]:
    """Shared core for the per-subnet kinds (attestations, columns, sync messages): each
    publish reaches exactly members(subnet) minus its publisher (`self_col` — origin for
    attestations/columns, attester for sync messages). `key_cols` is the kind's
    per-receiver dedup key, `miss_cols`/`leak_cols` its example-tuple layouts."""
    pubs = f"(SELECT DISTINCT slot, subnet, attester, origin FROM publishes WHERE kind = {kind})"
    arrivals = _arr_count(con, kind)
    published = _pub_count(con, kind)
    expected = _val(
        con,
        f"SELECT count(*) FROM {pubs} p JOIN {members} m USING (subnet) "
        f"WHERE m.node != p.{self_col}",
    )
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE miss_k{kind} AS
        SELECT {miss_cols} FROM {pubs} p JOIN {members} m USING (subnet)
        WHERE m.node != p.{self_col} AND NOT EXISTS (
            SELECT 1 FROM arrivals a WHERE a.kind = {kind} AND a.node = m.node
            AND a.slot = p.slot AND a.subnet = p.subnet
            AND a.attester = p.attester AND a.origin = p.origin)"""
    )
    leak_sql = (
        f"FROM arrivals a WHERE a.kind = {kind} AND NOT EXISTS "
        f"(SELECT 1 FROM {members} m WHERE m.subnet = a.subnet AND m.node = a.node)"
    )
    leaked = _val(con, f"SELECT count(*) {leak_sql}")
    leaked_examples = con.execute(
        f"SELECT {leak_cols} {leak_sql} ORDER BY ALL LIMIT 20"
    ).fetchall()
    dups = _dup_count(con, key_cols, kind)
    n_missing = _val(con, f"SELECT count(*) FROM miss_k{kind}")
    ok = not n_missing and not leaked and not dups and arrivals == expected
    rep = _kind_report(
        con, label, plural, kind,
        published=published, arrivals=arrivals, expected=expected,
        missing_table=f"miss_k{kind}", leaked=leaked, leaked_examples=leaked_examples,
        duplicates=dups, ok=ok, delays_sql=_delays_sql(kind), voted=voted,
    )
    return rep, ok


def _global_kind(
    con, label: str, plural: str, kind: int, published: set[tuple[int, int, int]], n: int
) -> tuple[dict, bool]:
    """_aggregate_result in SQL (aggregates / sync contributions / finality aggregates):
    each schedule-published (slot, subnet, aggregator) reaches every node but its
    aggregator; an arrival back at the aggregator is a leak."""
    _insert(con, f"sched_k{kind}", "slot BIGINT, subnet BIGINT, agg BIGINT", sorted(published))
    arrivals = _arr_count(con, kind)
    expected = len(published) * (n - 1)
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE miss_k{kind} AS
        SELECT r.node AS node, s.slot AS slot, s.subnet AS subnet, s.agg AS agg
        FROM sched_k{kind} s CROSS JOIN nodes_n r
        WHERE r.node != s.agg AND NOT EXISTS (
            SELECT 1 FROM arrivals a WHERE a.kind = {kind} AND a.node = r.node
            AND a.slot = s.slot AND a.subnet = s.subnet AND a.attester = s.agg)"""
    )
    leaked_examples = con.execute(
        f"SELECT DISTINCT node, slot, subnet, attester FROM arrivals "
        f"WHERE kind = {kind} AND node = attester ORDER BY ALL"
    ).fetchall()
    dups = _dup_count(con, "node, slot, subnet, attester", kind)
    n_missing = _val(con, f"SELECT count(*) FROM miss_k{kind}")
    ok = not n_missing and not leaked_examples and not dups and arrivals == expected
    rep = _kind_report(
        con, label, plural, kind,
        published=len(published), arrivals=arrivals, expected=expected,
        missing_table=f"miss_k{kind}", leaked=len(leaked_examples),
        leaked_examples=leaked_examples[:20], duplicates=dups, ok=ok,
        delays_sql=_delays_sql(kind),
    )
    return rep, ok


def _ac_votes(con, schedule_data: dict) -> tuple[dict, bool]:
    """analyze_ac_votes in SQL: each scheduled (slot, val, origin) vote reaches every node
    but its publisher; the -1 in example tuples is the unused subnet slot."""
    kind = ca.AC_VOTE_KIND
    n = schedule_data["params"]["n"]
    voters = [
        (sp["slot"], r["val"], r["node"])
        for sp in schedule_data["slots"]
        for r in sp.get("ac_voters") or []
    ]
    _insert(con, "sched_ac", "slot BIGINT, val BIGINT, origin BIGINT", voters)
    arrivals = _arr_count(con, kind)
    expected = len(voters) * (n - 1)
    n_voted = _val(
        con,
        f"SELECT count(*) FROM sched_ac s JOIN publishes p ON p.kind = {kind} "
        f"AND p.slot = s.slot AND p.attester = s.val AND p.origin = s.origin "
        f"AND p.voted_block",
    )
    fraction = n_voted / len(voters) if voters else 0.0
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE miss_k{kind} AS
        SELECT r.node AS node, s.slot AS slot, -1 AS subnet, s.val AS val, s.origin AS origin
        FROM sched_ac s CROSS JOIN nodes_n r
        WHERE r.node != s.origin AND NOT EXISTS (
            SELECT 1 FROM arrivals a WHERE a.kind = {kind} AND a.node = r.node
            AND a.slot = s.slot AND a.attester = s.val AND a.origin = s.origin)"""
    )
    leaked_examples = con.execute(
        f"SELECT DISTINCT node, slot, -1, attester, origin FROM arrivals "
        f"WHERE kind = {kind} AND node = origin ORDER BY ALL"
    ).fetchall()
    dups = _dup_count(con, "node, slot, attester, origin", kind)
    n_missing = _val(con, f"SELECT count(*) FROM miss_k{kind}")
    ok = not n_missing and not leaked_examples and not dups and arrivals == expected
    rep = _kind_report(
        con, "AC vote", "AC votes", kind,
        published=len(voters), arrivals=arrivals, expected=expected,
        missing_table=f"miss_k{kind}", leaked=len(leaked_examples),
        leaked_examples=leaked_examples[:20], duplicates=dups, ok=ok,
        delays_sql=_delays_sql(kind),
        voted=("fraction_voted_block", fraction),
    )
    return rep, ok


def _finality_votes(con, schedule_data: dict) -> tuple[dict, bool]:
    """analyze_finality_votes in SQL. The required/allowed receiver rules stay in
    check_arrivals._finality_receivers — only their evaluation over events moves here.
    Arrivals at allowed-but-not-required receivers are outside the strict accounting
    (not counted, not leaks); the headline CDF is over strict arrivals but the per-slot
    view (delay_details) is over ALL arrivals, exactly as in the reference."""
    kind = ca.FINALITY_VOTE_KIND
    required, allowed = ca._finality_receivers(schedule_data)
    pairs = con.execute(
        f"SELECT DISTINCT slot, subnet FROM arrivals WHERE kind = {kind} "
        f"UNION SELECT DISTINCT slot, subnet FROM publishes WHERE kind = {kind}"
    ).fetchall()
    req_rows = [(s, sub, nd) for s, sub in pairs for nd in required(s, sub)]
    alw_rows = [(s, sub, nd) for s, sub in pairs for nd in allowed(s, sub)]
    _insert(con, "fv_req", "slot BIGINT, subnet BIGINT, node BIGINT", req_rows)
    _insert(con, "fv_alw", "slot BIGINT, subnet BIGINT, node BIGINT", alw_rows)
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW fv_strict AS SELECT a.* FROM arrivals a "
        f"SEMI JOIN fv_req r ON r.slot = a.slot AND r.subnet = a.subnet AND r.node = a.node "
        f"WHERE a.kind = {kind}"
    )
    leak_sql = (
        f"FROM arrivals a WHERE a.kind = {kind} AND NOT EXISTS (SELECT 1 FROM fv_alw w "
        f"WHERE w.slot = a.slot AND w.subnet = a.subnet AND w.node = a.node)"
    )
    leaked = _val(con, f"SELECT count(*) {leak_sql}")
    leaked_examples = con.execute(
        f"SELECT node, slot, subnet, attester {leak_sql} ORDER BY ALL LIMIT 20"
    ).fetchall()
    pubs = f"(SELECT DISTINCT slot, subnet, attester, origin FROM publishes WHERE kind = {kind})"
    published = _pub_count(con, kind)
    arrivals = _val(con, "SELECT count(*) FROM fv_strict")
    expected = _val(
        con,
        f"SELECT count(*) FROM {pubs} p "
        f"JOIN fv_req r ON r.slot = p.slot AND r.subnet = p.subnet "
        f"WHERE r.node != p.origin",
    )
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE miss_k{kind} AS
        SELECT r.node AS node, p.slot AS slot, p.subnet AS subnet, p.attester AS val
        FROM {pubs} p JOIN fv_req r ON r.slot = p.slot AND r.subnet = p.subnet
        WHERE r.node != p.origin AND NOT EXISTS (
            SELECT 1 FROM fv_strict a WHERE a.node = r.node AND a.slot = p.slot
            AND a.subnet = p.subnet AND a.attester = p.attester AND a.origin = p.origin)"""
    )
    dups = _dup_count(con, "node, slot, subnet, attester, origin", kind, arrivals="fv_strict")
    n_missing = _val(con, f"SELECT count(*) FROM miss_k{kind}")
    ok = not n_missing and not leaked and not dups and arrivals == expected
    rep = _kind_report(
        con, "finality attestation", "finality attestations", kind,
        published=published, arrivals=arrivals, expected=expected,
        missing_table=f"miss_k{kind}", leaked=leaked, leaked_examples=leaked_examples,
        duplicates=dups, ok=ok,
        delays_sql=_delays_sql(kind, arrivals="fv_strict"),
        per_slot_sql=_delays_sql(kind),
    )
    return rep, ok


def _coverage_at_deadline(con, schedule_data: dict) -> dict[tuple[int, int], float]:
    """finality_coverage_at_deadline in SQL: per (round key, subnet), the fraction of
    published votes whose FIRST copy reached each aggregator host by that host's own
    aggregate-publish instant. Hosts come from the schedule draw; pairs whose host never
    published an aggregate have no deadline and are skipped (inner join)."""
    published, _n = ca._finality_aggregate_published(schedule_data)
    _insert(con, "fa_hosts", "slot BIGINT, subnet BIGINT, host BIGINT", sorted(published))
    rows = con.execute(
        f"""WITH votes AS (SELECT DISTINCT slot, subnet, attester AS val, origin
                           FROM publishes WHERE kind = {ca.FINALITY_VOTE_KIND}),
        agg_t AS (SELECT slot, subnet, attester AS host, min(t_ns) AS due
                  FROM publishes WHERE kind = {ca.FINALITY_AGGREGATE_KIND} GROUP BY ALL),
        first_arr AS (SELECT node, slot, subnet, attester, origin, min(t_ns) AS t
                      FROM arrivals WHERE kind = {ca.FINALITY_VOTE_KIND} GROUP BY ALL)
        SELECT v.slot, v.subnet, count(*),
               sum(CASE WHEN f.t IS NOT NULL AND f.t <= g.due THEN 1 ELSE 0 END)
        FROM votes v
        JOIN fa_hosts h ON h.slot = v.slot AND h.subnet = v.subnet AND h.host != v.origin
        JOIN agg_t g ON g.slot = v.slot AND g.subnet = v.subnet AND g.host = h.host
        LEFT JOIN first_arr f ON f.node = h.host AND f.slot = v.slot AND f.subnet = v.subnet
            AND f.attester = v.val AND f.origin = v.origin
        GROUP BY v.slot, v.subnet"""
    ).fetchall()
    return {(s, sub): covered / total for s, sub, total, covered in rows}


def build_report(run_dir: Path, parquet_dir: Path) -> dict:
    """Analyze a converted run: same stdout summary and analysis.json as the reference
    check_arrivals.build_report, computed from parquet_dir's event tables."""
    con = duckdb.connect()
    con.execute(
        "CREATE TEMP TABLE arrivals AS SELECT * FROM read_parquet(?)",
        [str(parquet_dir / "arrivals.parquet")],
    )
    con.execute(
        "CREATE TEMP TABLE publishes AS SELECT * FROM read_parquet(?)",
        [str(parquet_dir / "publishes.parquet")],
    )
    node_nums = json.loads((parquet_dir / "meta.json").read_text())["nodes"]
    _insert(con, "nodes", "node BIGINT", [(n,) for n in node_nums])

    report: dict = {"run_dir": str(run_dir), "nodes": len(node_nums), "kinds": {}}
    kinds = report["kinds"]
    print(f"nodes: {len(node_nums)}")
    blocks = _blocks(con, len(node_nums))
    ok = blocks["ok"]
    kinds["blocks"] = blocks

    # ePBS: both halves are block-shaped; the payload-lag headline mirrors the reference.
    if _pub_count(con, ca.CONSENSUS_BLOCK_KIND):
        rep = _blocks(con, len(node_nums), ca.CONSENSUS_BLOCK_KIND,
                      "consensus block", "consensus blocks")
        ok = ok and rep["ok"]
        kinds["consensus_blocks"] = rep
        rep = _blocks(con, len(node_nums), ca.EXECUTION_PAYLOAD_KIND,
                      "execution payload", "execution payloads")
        ok = ok and rep["ok"]
        kinds["execution_payloads"] = rep
        lag_cdf = _payload_lag_cdf(con)
        rep["payload_lag_ms"] = lag_cdf
        if lag_cdf["count"]:
            print(
                f"  payload lag CDF (ms): p50={lag_cdf['p50']:.1f} p90={lag_cdf['p90']:.1f} "
                f"p99={lag_cdf['p99']:.1f} p100={lag_cdf['p100']:.1f}"
            )

    schedule_path = run_dir / "schedule.json"
    if schedule_path.exists():
        schedule_data = json.loads(schedule_path.read_text())
        n = schedule_data["params"]["n"]
        _insert(con, "nodes_n", "node BIGINT", [(i,) for i in range(n)])
        _insert(
            con, "att_members", "subnet BIGINT, node BIGINT",
            [(s, nd) for s, mem in enumerate(schedule_data["subnet_subscribers"]) for nd in mem],
        )
        rep, k_ok = _membership_kind(
            con, "attestation", "attestations", ca.ATTEST_KIND, "att_members",
            self_col="origin",
            key_cols="node, slot, subnet, attester, origin",
            miss_cols="m.node, p.slot, p.subnet, p.attester, p.origin",
            leak_cols="a.node, a.slot, a.subnet, a.attester, a.origin",
            voted=("fraction_voted_block", _voted_fraction(con, ca.ATTEST_KIND)),
        )
        ok = ok and k_ok
        kinds["attestations"] = rep

        if schedule_data["slots"] and schedule_data["slots"][0].get("aggregators"):
            published, _ = ca._aggregate_published(schedule_data)
            rep, k_ok = _global_kind(
                con, "aggregate", "aggregates (distinct)", ca.AGGREGATE_KIND, published, n
            )
            ok = ok and k_ok
            kinds["aggregates"] = rep

        if schedule_data.get("column_subscribers"):
            _insert(
                con, "col_members", "subnet BIGINT, node BIGINT",
                [(c, nd) for c, mem in enumerate(schedule_data["column_subscribers"])
                 for nd in mem],
            )
            rep, k_ok = _membership_kind(
                con, "column", "columns (distinct)", ca.COLUMN_KIND, "col_members",
                self_col="origin",
                key_cols="node, slot, subnet, origin",
                miss_cols="m.node, p.slot, p.subnet, p.origin",
                leak_cols="a.node, a.slot, a.subnet, a.origin",
            )
            ok = ok and k_ok
            kinds["columns"] = rep

        if schedule_data.get("sync_subscribers"):
            _insert(
                con, "sync_members", "subnet BIGINT, node BIGINT",
                [(s, nd) for s, mem in enumerate(schedule_data["sync_subscribers"])
                 for nd in mem],
            )
            rep, k_ok = _membership_kind(
                con, "sync message", "sync messages", ca.SYNC_MESSAGE_KIND, "sync_members",
                self_col="attester",
                key_cols="node, slot, subnet, attester",
                miss_cols="m.node, p.slot, p.subnet, p.attester",
                leak_cols="a.node, a.slot, a.subnet, a.attester",
                voted=("fraction_voted_head", _voted_fraction(con, ca.SYNC_MESSAGE_KIND)),
            )
            ok = ok and k_ok
            kinds["sync_messages"] = rep

            if schedule_data["slots"] and schedule_data["slots"][0].get("sync_aggregators"):
                published, _ = ca._sync_contribution_published(schedule_data)
                rep, k_ok = _global_kind(
                    con, "sync contribution", "sync contributions (distinct)",
                    ca.SYNC_CONTRIBUTION_KIND, published, n,
                )
                ok = ok and k_ok
                kinds["sync_contributions"] = rep

        if schedule_data.get("finality_subscribers"):
            if schedule_data["slots"] and schedule_data["slots"][0].get("ac_voters"):
                rep, k_ok = _ac_votes(con, schedule_data)
                ok = ok and k_ok
                kinds["ac_votes"] = rep

            rep, k_ok = _finality_votes(con, schedule_data)
            ok = ok and k_ok
            kinds["finality_attestations"] = rep

            if any(sp.get("finality_aggregators") for sp in schedule_data["slots"]):
                published, _ = ca._finality_aggregate_published(schedule_data)
                rep, k_ok = _global_kind(
                    con, "finality aggregate", "finality aggregates (distinct)",
                    ca.FINALITY_AGGREGATE_KIND, published, n,
                )
                ok = ok and k_ok
                kinds["finality_aggregates"] = rep
                cov = _coverage_at_deadline(con, schedule_data)
                if cov:
                    by_slot: dict[str, dict[str, float]] = {}
                    for (slot, subnet), frac in sorted(cov.items()):
                        by_slot.setdefault(str(slot), {})[str(subnet)] = round(frac, 4)
                    kinds["finality_attestations"]["coverage_at_deadline"] = by_slot
                    print("  vote coverage at the aggregation deadline (per slot/subnet):")
                    for slot, subs in by_slot.items():
                        line = "  ".join(f"subnet {s}: {f:.3f}" for s, f in subs.items())
                        print(f"    slot {slot}: {line}")

        counts = schedule_data.get("validator_counts")
        if counts:
            top = sorted(enumerate(counts), key=lambda x: -x[1])[:10]
            report["validators"] = {
                "v": sum(counts),
                "max_per_node": max(counts),
                "top_hosts": [[nd, c] for nd, c in top],
                "counts_per_node": counts,
            }
            print(
                f"validators: V={sum(counts)} max/node={max(counts)} "
                "top hosts: " + " ".join(f"{nd}={c}" for nd, c in top[:5])
            )

    proposers = ca.load_proposers(run_dir)
    supernodes = ca.load_supernodes(run_dir)
    if proposers is not None and supernodes is not None:
        origins = dict(
            con.execute(
                f"SELECT slot, origin FROM publishes WHERE kind = {ca.BLOCK_KIND}"
            ).fetchall()
        )
        problems = ca.check_proposers(proposers, supernodes, origins)
        ok = ok and not problems
        report["proposer_guard"] = {"ok": not problems, "problems": problems[:20]}
        print(f"proposer guard: {'OK' if not problems else 'FAIL'} (all proposers are supernodes)")
        for p in problems[:10]:
            print("  ", p)

    con.close()
    report["result"] = "OK" if ok else "FAIL"
    (run_dir / "analysis.json").write_text(json.dumps(report, indent=2) + "\n")
    print("RESULT:", report["result"])
    return report
