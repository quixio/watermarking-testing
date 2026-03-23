#!/usr/bin/env python3
"""
Watermarking tests — CB (quixstreams 4.0.0a4, idle watermark advance)
                   vs CBNWM (quixstreams 3.23.1, no watermark advance).

  Test 1 — TGSR-first, then CB/CBNWM, then TGSR again (CBNWM flush via 2nd batch)
    Precondition : topics clean, CB/CBNWM OFF
    Steps        : TGSR -> start CB+CBNWM -> TGSR -> collect
    Expected     : CB emits all windows for both runs.
                   CBNWM emits run_1 windows (flushed by run_2 timestamps); run_2 stuck.

  Test 2 — Three pre-filled TGSR runs, then CB/CBNWM
    Precondition : topics clean, CB/CBNWM OFF
    Steps        : TGSR x 3 -> start CB+CBNWM -> collect
    Expected     : All 3 runs processed by CB.
                   CBNWM: run_1 & run_2 flushed by later timestamps; run_3 stuck.
                   Watch for CB getting stuck after first batch (known issue).

  Test 3 — CB/CBNWM running first, then two TGSR runs
    Precondition : topics clean, CB/CBNWM ON (idle, no data yet)
    Steps        : start CB+CBNWM -> TGSR -> TGSR -> collect
    Expected     : Both runs fully processed.
                   CBNWM flushes run_1 via run_2 timestamps.

  Test 4 — TGSR once, then CB/CBNWM, NO second batch (watermark proof)
    Precondition : topics clean, CB/CBNWM OFF
    Steps        : TGSR -> start CB+CBNWM -> wait (no more data)
    Expected     : CB idle-advances watermark -> all 200 windows emitted (2 M sum).
                   CBNWM watermark stuck -> 0 windows emitted.
                   Demonstrates watermarking prevents silent data loss.

Usage:
  python run_test.py [--test 1|2|3|4]   run specific test (default: 1)
  python run_test.py --collect           show current topic state only
  python run_test.py --collect-runs ID1 ID2 ...  collect & highlight specific run_ids
"""

import subprocess
import time
import json
import sys
import argparse

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

COMPOSE_FILE = "compose.local.yaml"
KAFKA_CONTAINER = "watermarkingtest-kafka_broker-1"

# Expected messages per TGSR run: 500 msgs x 20 brands x 20 colours x 10s
EXPECTED_MESSAGES_PER_RUN = 2_000_000
# Expected output records per run: 20 colours x 10 seconds = 200 windows
EXPECTED_RECORDS_PER_RUN = 200

# Wait times (seconds) — adjust if your machine is slow/fast
KAFKA_STARTUP_WAIT   = 15    # broker readiness after docker up
TGSR_STARTUP_SLACK   = 5     # extra seconds after compose-run returns for container teardown

# After starting CB/CBNWM against PRE-EXISTING data:
#   needs: consume all msgs (~5-15s) + idle_partition_timeout (~30s) + advance (~20s) + buffer
WAIT_AFTER_START_CB  = 90

# After second TGSR while CB/CBNWM are running (flush/advance scenario):
#   needs: consume new msgs + process + flush old windows
WAIT_AFTER_TGSR_FLUSH = 70

# Test 4 only — extra wait to let CB idle-advance (no second TGSR)
WAIT_CB_IDLE_ADVANCE = 90


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_cmd(cmd, *, check=True, capture=False, description=None):
    label = description or cmd[:80]
    print(f"  $ {label}", flush=True)
    return subprocess.run(cmd, shell=True, check=check,
                          capture_output=capture, text=capture)


def wait_countdown(seconds, label=""):
    msg = f"  Waiting {seconds}s"
    if label:
        msg += f" — {label}"
    print(msg, flush=True)
    step = 15
    remaining = seconds
    while remaining > 0:
        print(f"    {remaining}s remaining …", flush=True)
        time.sleep(min(step, remaining))
        remaining -= step


def consume_topic(topic, timeout_s=30):
    """Read all messages from a topic (offset 0 to end)."""
    result = subprocess.run(
        ["docker", "exec", KAFKA_CONTAINER,
         "rpk", "topic", "consume", topic,
         "--offset", "0:end",
         "-f", "%v\n"],
        capture_output=True, text=True, timeout=timeout_s + 15,
    )
    records = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                records.append(json.loads(line))
            except Exception:
                pass
    return records


def extract_run_id(output_text):
    """Parse run_id from TGSR output."""
    for line in output_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("run_id=run_"):
            return stripped[len("run_id="):]
    # Fallback: grab from "Produced second … run_id=…" lines
    for line in output_text.splitlines():
        if "run_id=" in line and "run_2" in line:
            idx = line.index("run_id=") + len("run_id=")
            tail = line[idx:]
            if ", generation took:" in tail:
                return tail[:tail.index(", generation took:")]
            return tail.split()[0]
    return None


# ---------------------------------------------------------------------------
# Infrastructure control
# ---------------------------------------------------------------------------

def clean_and_start_kafka():
    """Tear down everything (incl. state volumes) and start a fresh Kafka broker."""
    print("\n[SETUP] Full cleanup — removing containers and state volumes …")
    run_cmd(f'docker compose -f {COMPOSE_FILE} down -v --remove-orphans', check=False)

    print("[SETUP] Starting Kafka (Redpanda) …")
    run_cmd(f'docker compose -f {COMPOSE_FILE} up -d kafka_broker')
    wait_countdown(KAFKA_STARTUP_WAIT, "broker readiness")

    print("[SETUP] Creating highway-2 with 4 partitions …")
    run_cmd(
        f'docker exec {KAFKA_CONTAINER} rpk topic create highway-2 --partitions 4 --replicas 1',
        description="rpk topic create highway-2"
    )


def start_cb_cbnwm():
    print("\n  Starting CB and CBNWM …")
    run_cmd(
        f'docker compose -f {COMPOSE_FILE} up -d --build croatian_burocrats croatian_burocrats_nowm',
        description="docker compose up CB + CBNWM"
    )


def stop_cb_cbnwm():
    print("\n  Stopping CB and CBNWM …")
    run_cmd(
        f'docker compose -f {COMPOSE_FILE} stop croatian_burocrats croatian_burocrats_nowm',
        check=False,
        description="docker compose stop CB + CBNWM"
    )


def start_tgcon():
    print("\n  Starting TGCon (continuous traffic generator) …")
    run_cmd(
        f'docker compose -f {COMPOSE_FILE} up -d --build traffic_generator_continuous',
        description="docker compose up traffic_generator_continuous"
    )


def stop_tgcon():
    print("\n  Stopping TGCon …")
    run_cmd(
        f'docker compose -f {COMPOSE_FILE} stop traffic_generator_continuous',
        check=False,
        description="docker compose stop traffic_generator_continuous"
    )


def run_tgsr(label="") -> str:
    """Run TGSR (single-run traffic generator) and return the run_id."""
    tag = f" [{label}]" if label else ""
    print(f"\n  Running TGSR{tag} …", flush=True)
    result = run_cmd(
        f'docker compose -f {COMPOSE_FILE} run --rm traffic_generator_singlerun',
        capture=True,
        description="docker compose run traffic_generator_singlerun"
    )
    output = result.stdout + result.stderr
    print(output, flush=True)
    run_id = extract_run_id(output)
    if run_id:
        print(f"  TGSR{tag} done — run_id = {run_id}", flush=True)
    else:
        print(f"  WARNING: could not extract run_id from TGSR{tag} output", flush=True)
    time.sleep(TGSR_STARTUP_SLACK)
    return run_id


# ---------------------------------------------------------------------------
# Results display
# ---------------------------------------------------------------------------

def aggregate(records):
    """Return (totals_by_run_id, record_counts_by_run_id)."""
    totals, counts = {}, {}
    for r in records:
        rid = r.get("run_id", "<unknown>")
        totals[rid] = totals.get(rid, 0) + r.get("count", 0)
        counts[rid] = counts.get(rid, 0) + 1
    return totals, counts


def display_results(cb_records, nwm_records, highlight_ids=None, test_label=""):
    cb_totals,  cb_counts  = aggregate(cb_records)
    nwm_totals, nwm_counts = aggregate(nwm_records)
    highlight_ids = highlight_ids or []

    all_ids = sorted(set(list(cb_totals) + list(nwm_totals)))

    W = 108
    print()
    if test_label:
        print(f"  {'-' * (W - 4)}  {test_label}")
    print("=" * W)
    print(
        f"  {'run_id':<48}"
        f"{'CB recs':>8}  {'CB sum(count)':>14}"
        f"  {'CBNWM recs':>10}  {'CBNWM sum(count)':>16}"
        f"  {'notes'}"
    )
    print("-" * W)

    for rid in all_ids:
        cb_r = cb_counts.get(rid, 0)
        cb_s = cb_totals.get(rid, 0)
        nw_r = nwm_counts.get(rid, 0)
        nw_s = nwm_totals.get(rid, 0)

        notes = []
        if rid in highlight_ids:
            notes.append("<<< test run")
        if cb_s == EXPECTED_MESSAGES_PER_RUN:
            notes.append("CB OK")
        elif cb_s > 0:
            pct = 100.0 * cb_s / EXPECTED_MESSAGES_PER_RUN
            notes.append(f"CB {pct:.0f}%")
        else:
            notes.append("CB —")
        if nw_s == EXPECTED_MESSAGES_PER_RUN:
            notes.append("CBNWM OK")
        elif nw_s > 0:
            pct = 100.0 * nw_s / EXPECTED_MESSAGES_PER_RUN
            notes.append(f"CBNWM {pct:.0f}%")
        else:
            notes.append("CBNWM stuck (0)")

        print(
            f"  {rid[:47]:<48}"
            f"{cb_r:>8,}  {cb_s:>14,}"
            f"  {nw_r:>10,}  {nw_s:>16,}"
            f"  {', '.join(notes)}"
        )

    print("=" * W)
    print(f"  Totals:"
          f"  CB  {len(cb_records):,} records, sum(count) = {sum(cb_totals.values()):,}"
          f"  |  CBNWM  {len(nwm_records):,} records, sum(count) = {sum(nwm_totals.values()):,}")
    print()


def collect_and_display(highlight_ids=None, test_label=""):
    print("\n  Reading 'colours'    topic (CB  — watermarks) …", flush=True)
    cb_records = consume_topic("colours")
    print("  Reading 'coloursNoWM' topic (CBNWM — no watermarks) …", flush=True)
    nwm_records = consume_topic("coloursNoWM")
    display_results(cb_records, nwm_records, highlight_ids=highlight_ids, test_label=test_label)
    return cb_records, nwm_records


# ---------------------------------------------------------------------------
# Test 1 — TGSR first, then CB/CBNWM, then second TGSR (CBNWM flush)
# ---------------------------------------------------------------------------

def test_1():
    print()
    print("=" * 70)
    print("  TEST 1 — Pre-existing data, then CBNWM flushed by second TGSR batch")
    print("=" * 70)
    print("  Flow: TGSR(run_1) -> start CB+CBNWM -> TGSR(run_2) -> collect")
    print("  CBNWM: run_1 windows closed by run_2 timestamps; run_2 windows stuck.")
    print("  CB   : idle watermark advance closes all windows for both runs.")

    clean_and_start_kafka()

    # Step 1: pre-fill with run_1 (CB/CBNWM are OFF)
    print("\n[1/4] Pre-filling highway-2 with run_1 (CB/CBNWM are OFF) …")
    run_id_1 = run_tgsr("run_1")

    # Step 2: start CB and CBNWM
    print("\n[2/4] Starting CB and CBNWM …")
    start_cb_cbnwm()
    wait_countdown(WAIT_AFTER_START_CB,
                   "CB/CBNWM consuming run_1 + idle timeout + watermark advance")

    # Step 3: second TGSR to flush CBNWM's stuck windows
    print("\n[3/4] Running TGSR run_2 to advance watermarks (flushes CBNWM run_1 windows) …")
    run_id_2 = run_tgsr("run_2")
    wait_countdown(WAIT_AFTER_TGSR_FLUSH,
                   "CB/CBNWM processing run_2 + flushing run_1 windows")

    # Step 4: collect
    print("\n[4/4] Collecting results …")
    cb_records, nwm_records = collect_and_display(
        highlight_ids=[run_id_1, run_id_2],
        test_label="Test 1"
    )

    # Analysis
    cb_t, _ = aggregate(cb_records)
    nw_t, _ = aggregate(nwm_records)
    print("  Analysis:")
    for rid, label in [(run_id_1, "run_1"), (run_id_2, "run_2")]:
        cb_s = cb_t.get(rid, 0)
        nw_s = nw_t.get(rid, 0)
        cb_ok = "PASS" if cb_s == EXPECTED_MESSAGES_PER_RUN else f"FAIL ({cb_s:,} / {EXPECTED_MESSAGES_PER_RUN:,})"
        nw_note = "PASS" if nw_s == EXPECTED_MESSAGES_PER_RUN else (
            f"{nw_s:,} / {EXPECTED_MESSAGES_PER_RUN:,} — expected stuck (no watermark advance)" if label == "run_2"
            else f"PASS" if nw_s == EXPECTED_MESSAGES_PER_RUN else f"PARTIAL {nw_s:,}"
        )
        print(f"    {label}: CB={cb_ok}  CBNWM={nw_note}")
    print()


# ---------------------------------------------------------------------------
# Test 2 — Three TGSR runs, then CB/CBNWM (CB stuck detection)
# ---------------------------------------------------------------------------

def test_2():
    print()
    print("=" * 70)
    print("  TEST 2 — Three pre-filled TGSR runs, then start CB+CBNWM")
    print("=" * 70)
    print("  Flow: TGSRx3 -> start CB+CBNWM -> collect")
    print("  CBNWM: run_1 & run_2 windows closed by later timestamps; run_3 stuck.")
    print("  CB   : all 3 runs fully processed (idle advance).")
    print("  Watch: CB may get stuck after processing the first batch (known issue).")

    clean_and_start_kafka()

    # Step 1-3: three TGSR runs, CB/CBNWM still OFF
    print("\n[1/5] Running TGSR three times (CB/CBNWM are OFF) …")
    run_id_1 = run_tgsr("run_1")
    run_id_2 = run_tgsr("run_2")
    run_id_3 = run_tgsr("run_3")
    run_ids = [run_id_1, run_id_2, run_id_3]
    print(f"\n  All 3 TGSR runs complete:")
    for i, rid in enumerate(run_ids, 1):
        print(f"    run_{i}: {rid}")

    # Step 4: start CB and CBNWM
    print("\n[2/5] Starting CB and CBNWM …")
    start_cb_cbnwm()

    # Longer wait — CB needs to process 6M messages + idle out run_3 windows
    # 3 runs × ~55s each to consume + watermark advance buffer + EOS commit time
    # Both partitions fire near T+165s; EOS commit adds ~20s → need T+190s minimum.
    # Use 240s to give a comfortable buffer for EOS commits on both partitions.
    wait_after = max(WAIT_AFTER_START_CB, 240)
    wait_countdown(wait_after,
                   "CB/CBNWM consuming 3 runs (6M msgs) + idle timeout + advance")

    # Step 5: collect
    print("\n[3/5] Collecting results …")
    cb_records, nwm_records = collect_and_display(
        highlight_ids=run_ids,
        test_label="Test 2"
    )

    # Analysis
    cb_t, cb_c = aggregate(cb_records)
    nw_t, nw_c = aggregate(nwm_records)

    print("  Analysis:")
    all_processed_by_cb = True
    for i, rid in enumerate(run_ids, 1):
        cb_s = cb_t.get(rid, 0)
        nw_s = nw_t.get(rid, 0)
        cb_ok = cb_s == EXPECTED_MESSAGES_PER_RUN
        if not cb_ok:
            all_processed_by_cb = False
        cb_str = "PASS" if cb_ok else f"FAIL/STUCK — {cb_s:,} / {EXPECTED_MESSAGES_PER_RUN:,}"
        is_last = (i == 3)
        nw_str = (
            "PASS" if nw_s == EXPECTED_MESSAGES_PER_RUN
            else f"STUCK (0) — expected, last run has no flush" if (is_last and nw_s == 0)
            else f"PARTIAL {nw_s:,} / {EXPECTED_MESSAGES_PER_RUN:,}"
        )
        print(f"    run_{i}: CB={cb_str}  CBNWM={nw_str}")

    if not all_processed_by_cb:
        print()
        print("  -- CB DID NOT PROCESS ALL RUNS — likely stuck after first batch --")
        print("  -- This is the known CB-stuck-after-first-batch bug.             --")
    else:
        print()
        print("  CB processed all 3 runs correctly.")
    print()


# ---------------------------------------------------------------------------
# Test 3 — CB/CBNWM ON first, then two TGSR runs
# ---------------------------------------------------------------------------

def test_3():
    print()
    print("=" * 70)
    print("  TEST 3 — CB/CBNWM running first, two TGSR runs")
    print("=" * 70)
    print("  Flow: start CB+CBNWM (idle) -> TGSR(run_1) -> TGSR(run_2) -> collect")
    print("  All messages should be processed: CBNWM flushes run_1 via run_2 timestamps.")

    clean_and_start_kafka()

    # Step 1: start CB and CBNWM (they idle-wait for data)
    print("\n[1/4] Starting CB and CBNWM (will idle until data arrives) …")
    start_cb_cbnwm()
    wait_countdown(10, "CB/CBNWM startup")

    # Step 2: first TGSR
    print("\n[2/4] Running TGSR run_1 …")
    run_id_1 = run_tgsr("run_1")
    wait_countdown(WAIT_AFTER_TGSR_FLUSH,
                   "CB/CBNWM consuming run_1 (CBNWM windows will be flushed by run_2)")

    # Step 3: second TGSR — flushes CBNWM run_1 windows
    print("\n[3/4] Running TGSR run_2 (advances watermarks, flushes run_1 windows) …")
    run_id_2 = run_tgsr("run_2")
    wait_countdown(WAIT_AFTER_TGSR_FLUSH,
                   "CB/CBNWM consuming run_2 + idle advance for run_2 windows")

    # Step 4: collect
    print("\n[4/4] Collecting results …")
    cb_records, nwm_records = collect_and_display(
        highlight_ids=[run_id_1, run_id_2],
        test_label="Test 3"
    )

    # Analysis
    cb_t, _ = aggregate(cb_records)
    nw_t, _ = aggregate(nwm_records)
    print("  Analysis:")
    for i, rid in enumerate([run_id_1, run_id_2], 1):
        cb_s = cb_t.get(rid, 0)
        nw_s = nw_t.get(rid, 0)
        cb_ok = "PASS" if cb_s == EXPECTED_MESSAGES_PER_RUN else f"FAIL {cb_s:,}"
        nw_str = (
            "PASS" if nw_s == EXPECTED_MESSAGES_PER_RUN
            else f"STUCK — run_2 has no follow-up flush" if (i == 2 and nw_s == 0)
            else f"PARTIAL {nw_s:,}"
        )
        print(f"    run_{i}: CB={cb_ok}  CBNWM={nw_str}")
    print()


# ---------------------------------------------------------------------------
# Test 4 — Online stream: CB+CBNWM running, then TGCon started
# ---------------------------------------------------------------------------

# How long to let TGCon run before collecting results.
# Window=1s, grace=10s → first windows close after ~11s of data.
# 60s gives many windows time to emit from both CB and CBNWM.
WAIT_TGCON_ONLINE = 60


def test_4():
    print()
    print("=" * 70)
    print("  TEST 4 — Online stream: CB+CBNWM running, TGCon started after")
    print("=" * 70)
    print("  Precondition: fresh topics, CB and CBNWM already running (idle).")
    print("  Flow: start CB+CBNWM -> start TGCon -> wait -> collect -> stop TGCon")
    print()
    print("  Expected: with continuous traffic both CB and CBNWM produce records.")
    print("  Watermarks close naturally as new timestamps advance past grace period.")

    clean_and_start_kafka()

    # Step 1: start CB and CBNWM (idle, no data yet)
    print("\n[1/3] Starting CB and CBNWM (will idle until data arrives) …")
    start_cb_cbnwm()
    wait_countdown(15, "CB/CBNWM startup")

    # Step 2: start continuous traffic generator
    print("\n[2/3] Starting TGCon (continuous traffic) …")
    start_tgcon()
    wait_countdown(WAIT_TGCON_ONLINE,
                   "TGCon producing; windows closing as timestamps advance (grace=10s)")

    # Step 3: collect
    print("\n[3/3] Collecting results …")
    cb_records, nwm_records = collect_and_display(test_label="Test 4 — Online stream")

    stop_tgcon()

    # Analysis
    cb_t, cb_c = aggregate(cb_records)
    nw_t, nw_c = aggregate(nwm_records)
    total_cb  = sum(cb_t.values())
    total_nwm = sum(nw_t.values())

    print("  Analysis:")
    print(f"    CB    : {len(cb_records):>6,} records, sum(count) = {total_cb:>12,}  "
          f"{'PRODUCING' if total_cb > 0 else 'STUCK — no records emitted'}")
    print(f"    CBNWM : {len(nwm_records):>6,} records, sum(count) = {total_nwm:>12,}  "
          f"{'PRODUCING' if total_nwm > 0 else 'STUCK — no records emitted'}")
    if total_cb > 0 and total_nwm > 0:
        print("\n  PASS — both services are producing output from the live stream.")
    elif total_cb > 0 and total_nwm == 0:
        print("\n  WARN — CB producing but CBNWM stuck. Check CBNWM logs.")
    else:
        print("\n  FAIL — neither service produced output. Check CB and CBNWM logs.")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="CB vs CBNWM watermarking tests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--test", choices=["1", "2", "3", "4"], default="1",
                       help="Which test to run (default: 1)")
    group.add_argument("--collect", action="store_true",
                       help="Just collect + display current topic state")
    parser.add_argument("--collect-runs", nargs="*", metavar="RUN_ID",
                        help="Highlight specific run_ids in --collect output")
    args = parser.parse_args()

    if args.collect:
        print("\n  Collecting current topic state …")
        collect_and_display(highlight_ids=args.collect_runs or [])
        return

    tests = {"1": test_1, "2": test_2, "3": test_3, "4": test_4}
    tests[args.test]()


if __name__ == "__main__":
    main()
