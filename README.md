# WatermarkingTest

A Quix Streams pipeline for testing event-time watermarking with simulated vehicle traffic data.

## Pipeline

```
Traffic Generator ─► Croatian Burocrats ─► colours topic
    (highway-2)       (4 replicas, CB)
                  ─► Croatian Burocrats NWM ─► coloursNoWM topic
                       (4 replicas, CBNWM)
```

---

## Components

### Traffic Generator (TGSR)
Quix Streams **Source** (single-run Job). Generates 2,000,000 simulated vehicle messages over 10 seconds.

- 500 messages × 20 brands × 20 colours × 10 seconds = 2,000,000 messages/run
- Each message: `plate`, `brand`, `colour`, `passengers`, `ts_start`, `ts` (epoch ms)
- Message key is the vehicle brand
- Topic: `highway-2` (4 partitions)

---

### Croatian Burocrats (CB)
Quix Streams **Service** — **4 replicas**, quixstreams **4.0.0a7** (patched wheel).

Consumes `highway-2`, repartitions by colour (`group_by("colour")`), runs 1-second tumbling windows with **event-time watermarking**, and publishes window results to `colours`.

**Watermarking mechanism:**
- `watermarks_idle_advance_after_ms=30000` — if global watermark hasn't advanced for 30 s, force-advance to wall-clock now and flush pending windows.
- `watermarks_idle_partition_timeout=30.0` — exclude owned TPs still at watermark -1 for > 30 s from the global min.
- `eos_stable_seconds=15.0` — wait 15 s before seeking past EOS transaction control records.

**Environment variables:**

| Variable | Description |
|----------|-------------|
| `input` | Input topic (`highway-2`) |
| `output` | Output topic (`colours`) |
| `consumer_group` | Consumer group suffix (e.g. `v1`) |

---

### Croatian Burocrats NWM (CBNWM)
Quix Streams **Service** — **4 replicas**, quixstreams **3.23.1** (no watermarking).

Same windowing pipeline as CB but without the idle watermark advance. Used as a reference: run_N windows stay stuck unless a later run flushes them with higher timestamps.

---

## Topic Layout

| Topic | Partitions | Notes |
|-------|-----------|-------|
| `highway-2` | **4** | Input; created manually before CB starts |
| `repartition__burocrats_watermarking_v1--highway-2--colour` | **4** | Auto-created; derived from highway-2 |
| `watermarks__burocrats_watermarking_v1--watermarks` | **1** | All CB replicas subscribe + produce here |
| `colours` | auto | CB window output |
| `coloursNoWM` | auto | CBNWM window output |

---

## Running the Tests

```bash
# Run all 4 tests sequentially
python run_test.py

# Run a specific test
python run_test.py --test 1

# Just check current topic state
python run_test.py --collect
```

### Test Descriptions

| Test | Flow | Expected CB Result |
|------|------|--------------------|
| 1 | TGSR(run_1) → start CB+CBNWM → TGSR(run_2) → collect | Both runs fully flushed via idle advance |
| 2 | TGSR × 3 → start CB+CBNWM → collect | All 3 runs flushed |
| 3 | Start CB+CBNWM → TGSR(run_1) → TGSR(run_2) → collect | Both runs flushed |
| 4 | TGSR(run_1) → start CB+CBNWM → wait (no second batch) | Idle-advance flushes all 200 windows, 2M records |

---

## Patched Wheel

CB uses a patched quixstreams 4.0.0a7 wheel located at `croatian-burocrats/quixstreams-4.0.0a7-py3-none-any.whl`.

The wheel is built from `C:\repos\quix-streams_4a4` and includes all fixes. Patch files (kept for reference) are in `croatian-burocrats/patch_*.py`.

To rebuild the wheel after source changes:

```bash
cd C:/repos/quix-streams_4a4
pip wheel . --no-deps -w dist/
cp dist/quixstreams-4.0.0a7-py3-none-any.whl ../WatermarkingTest/croatian-burocrats/
```

---

## Architecture Notes

See [KNOWLEDGEBASE.md](KNOWLEDGEBASE.md) for detailed debugging history, EOS fix rationale, and watermarking mechanism documentation.
