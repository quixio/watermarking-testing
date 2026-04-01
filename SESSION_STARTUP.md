# Session Startup Guide — WatermarkingTest

> Read this at the start of every new testing session before touching any code.

---

## What Each Component Is

| Abbreviation | Full Name | Role |
|---|---|---|
| **TGSR** | Traffic Generator Single Run | Produces exactly 2,000,000 messages to `highway-2` over 10 seconds, then stops. Generates one `run_id`. Used in all 4 tests. |
| **TGCon** | Traffic Generator Continuous | Produces messages to `highway-2` indefinitely (1 msg/brand/colour/second). Used in **Test 4** and for Quix Cloud manual testing. |
| **CB** | Croatian Burocrats | The main service under test. Quixstreams **4.0.0a8 patched wheel**. 4 replicas. Reads `highway-2`, `group_by("colour")`, tumbling 1s window + 10s grace, outputs to `colours`. Has idle watermark advance. |
| **CBNWM** | Croatian Burocrats No WaterMark | Reference/comparison service. Quixstreams **3.23.1** (unpatched, published). 4 replicas. Same pipeline as CB but no watermark advance — so last few windows per run stay stuck unless a later run flushes them. Outputs to `coloursNoWM`. |

---

## Topology (must match this everywhere)

| Topic | Partitions | Who writes | Who reads |
|-------|-----------|------------|-----------|
| `highway-2` | **4** | TGSR / TGCon | CB, CBNWM |
| `repartition__burocrats_watermarking_v1--highway-2--colour` | **4** | CB (internal group_by) | CB (2nd stage) |
| `watermarks__burocrats_watermarking_v1--watermarks` | **1** | All CB replicas | All CB replicas |
| `colours` | auto | CB | downstream |
| `coloursNoWM` | auto | CBNWM | downstream |

**CB replicas = 4 (one per highway-2 partition)**
Each CB replica owns exactly: `highway-2[N]` + `repartition[N]` + `watermarks[0]`

---

## The 4 Tests

Run with: `python run_test.py --test N`
Tests 1–3 output measured by `sum(count)` per `run_id` in `colours` / `coloursNoWM`.
Expected for TGSR runs: **200 records** and **sum(count) = 2,000,000** per run (20 colours × 10 windows × 10,000 msgs/window).

### Test 1
**Precondition:** topics cleaned, CB and CBNWM off.

1. Run TGSR (run_1) — wait for it to finish
2. Start CB + CBNWM — wait for them to finish processing run_1
3. Run TGSR (run_2)
4. Check `sum(count)` in `colours` and `coloursNoWM` per run_id; put results in a table.

| CB Expected | CBNWM Expected |
|-------------|----------------|
| run_1 PASS, run_2 PASS | run_1 PASS (flushed by run_2 timestamps), run_2 partial/stuck |

---

### Test 2
**Precondition:** topics cleaned, CB and CBNWM off.

1. Run TGSR (run_1) — wait for finish
2. Run TGSR (run_2) — wait for finish
3. Run TGSR (run_3) — wait for finish
4. Start CB + CBNWM
5. Check `sum(count)` for all 3 runs. Note: CB sometimes gets stuck after processing the first batch — verify all 3 runs were fully processed.

| CB Expected | CBNWM Expected |
|-------------|----------------|
| All 3 runs PASS | run_1+2 PASS, run_3 stuck |

---

### Test 3
**Precondition:** topics cleaned, CB and CBNWM already running.

1. Run TGSR (run_1)
2. Run TGSR (run_2)
3. Check results — all messages should be processed by the watermarking version (CB).

| CB Expected | CBNWM Expected |
|-------------|----------------|
| Both runs PASS | run_1 PASS, run_2 stuck |

---

### Test 4
**Precondition:** CB and CBNWM already running.

1. Start **TGCon** (continuous traffic generator — no run number, runs indefinitely)
2. Verify that both CB and CBNWM start producing data to their output topics.

| CB Expected | CBNWM Expected |
|-------------|----------------|
| Continuously producing records to `colours` | Continuously producing records to `coloursNoWM` |

---

## How to Rebuild the Wheel

The patched wheel is at:
```
C:\repos\WatermarkingTest\croatian-burocrats\quixstreams-4.0.0a8-py3-none-any.whl
```

Built from source at:
```
C:\repos\quix-streams_4a4\
```

**Rebuild command (run from anywhere):**
```bash
cd C:/repos/quix-streams_4a4
pip wheel . --no-deps -w dist/
cp dist/quixstreams-4.0.0a8-py3-none-any.whl ../WatermarkingTest/croatian-burocrats/
```

**Always rebuild the wheel after editing source files in `C:\repos\quix-streams_4a4\`.**

---

## Running the Tests

```bash
cd C:/repos/WatermarkingTest

# Run one test
python run_test.py --test 1

# Run all 4 in sequence
python run_test.py

# Just read current topic state (no test execution)
python run_test.py --collect
```

Prerequisites:
- Docker Desktop running
- `compose.local.yaml` intact (Kafka broker + CB×4 + CBNWM×4 + TGSR)
- Wheel at `croatian-burocrats/quixstreams-4.0.0a8-py3-none-any.whl` is up to date

---

## CB Application Parameters (main.py)

```python
app = Application(
    consumer_group="burocrats_watermarking_" + os.environ["consumer_group"],
    auto_create_topics=True,
    auto_offset_reset="earliest",
    processing_guarantee="exactly-once",
    commit_every=1000,
    max_partition_buffer_size=10000,
    commit_interval=10,                         # seconds between EOS commits
    eos_stable_seconds=15.0,                    # debounce > commit_interval
    watermarks_idle_partition_timeout=30.0,     # exclude -1 partitions after 30s
    watermarks_idle_advance_after_ms=30000,     # force advance after 30s no-progress
)
```

---

## Key Known Bugs (already fixed in wheel)

| Bug | Symptom | Fix |
|-----|---------|-----|
| EOS stuck | CB=0 records; `pos < high` forever on repartition partitions | **Unresolved** — no fix in current wheel |
| `idle_watermark` unset | `UnboundLocalError` crash on startup, CB=0 | `idle_watermark = None` before `while` loop in `app.py` |
| Watermarks topic N partitions | Replicas without `watermarks[0]` never receive watermark updates | `manager.py` forces watermarks topic to 1 partition |
| Non-blocking watermarks buffer | Watermarks partition blocks data `pop()` when empty | `buffering.py` marks watermarks partition `non_blocking=True` |
| Stale state + offset reset | After redeploy, all replayed data classified as "late" → 0 output | Increase topic retention; delete/recreate topics before restart |
| Watermark starvation | At high data volume (400K msgs/sec), watermark messages never consumed → windows only close once via idle-advance | Reduce `MESSAGES_PER_BRAND_COLOUR` or implement priority watermark consumption |
| `highway-2` retention too small | 50MB `retentionInBytes` fills in <1 min at high volume → offset out of range | Changed to `retentionInMinutes: 60, retentionInBytes: -1` |

## Quix Cloud — Known Issues

### Issue 1: Stale state + offset reset (Bug 3)
**Symptom**: After redeploy, CB logs 39K+ "Skipping record processing for the closed window" messages and emits zero output.
**Cause**: `highway-2` retention (was 50MB) deletes segments → consumer offset invalid → reset to BEGINNING → stale `latest_expired_window_end` in RocksDB classifies all replayed data as "late".
**Fix**: `retentionInMinutes: 60, retentionInBytes: -1` in `quix.yaml`. Delete/recreate topics if already in bad state.

### Issue 2: Watermark starvation at high volume (Bug 4)
**Symptom**: CB fires idle-advance once, then no more window expiry despite continuous data. CBNWM works fine.
**Cause**: TGCon `MESSAGES_PER_BRAND_COLOUR=1000` → 400K data msgs/sec starves ~8 watermark msgs/sec in `poll()`.
**Fix**: Reduce `MESSAGES_PER_BRAND_COLOUR` or implement library-level watermark priority.

### Issue 3: Stale Docker image (historical, fixed)
**Symptom**: CB crashes immediately with `UnboundLocalError: idle_watermark`.
**Fix**: Force redeploy to rebuild image with current wheel.

**How to verify fix is deployed**: Check CB logs — if you see `[STARTUP] calling app.run()` followed by normal processing, the fix is in. If you see `UnboundLocalError: cannot access local variable 'idle_watermark'`, the old image is still running.

---

## File Map

```
C:\repos\WatermarkingTest\
  run_test.py                          ← test runner (all 4 tests)
  compose.local.yaml                   ← Docker Compose: Kafka + CB×4 + CBNWM×4 + TGSR
  KNOWLEDGEBASE.md                     ← deep debugging history and EOS/watermark docs
  SESSION_STARTUP.md                   ← THIS FILE
  croatian-burocrats\
    main.py                            ← CB application entry point
    quixstreams-4.0.0a8-py3-none-any.whl  ← patched wheel (deploy this)
  croatian-burocrats-nowm\
    main.py                            ← CBNWM application (quixstreams 3.23.1, no watermarks)

C:\repos\quix-streams_4a4\
  quixstreams\app.py                   ← patched source (edit here, then rebuild wheel)
  quixstreams\internal_consumer\
    consumer.py                        ← patched source
    buffering.py                       ← patched source
  quixstreams\models\topics\
    manager.py                         ← patched source
  quixstreams\processing\
    watermarking.py                    ← patched source
  dist\                                 ← wheel built here, then copy to croatian-burocrats\
```
