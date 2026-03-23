# WatermarkingTest – Debugging Knowledge Base

## Pipeline Architecture

### Components
- **TGSR** (Traffic Generator Single Run): produces 2,000,000 messages per run to `highway-2` topic (4 partitions).
- **CB** (Croatian Bureaucrats): 4 replicas; consumes `highway-2`, repartitions by colour via `group_by("colour")` into `repartition` topic (4 partitions), runs 1-second tumbling windows, outputs to `output` topic.
- **CBNWM** (CB No Watermark Manager): 4 replicas; same as CB but without watermarking logic (quixstreams 3.23.1).
- **quixstreams version**: 4.0.0a8 with `processing_guarantee="exactly-once"` (EOS/transactional Kafka).

### Topic Layout
| Topic | Partitions | Notes |
|-------|-----------|-------|
| `highway-2` | **4** | Input; TGSR writes here; must match CB replica count |
| `repartition__burocrats_watermarking_v1--highway-2--colour` | **4** | Internal; CB group_by output; derived from highway-2 |
| `watermarks__burocrats_watermarking_v1--watermarks` | **1** | Watermarks; all replicas subscribe and produce here |
| `output` (`colours`) | variable | CB window results |

**Note on repartition count**: 4 partitions are derived automatically from highway-2. An alternative of 1 partition (via `_single_partition_groupby`) would eliminate repartition EOS gaps but would lose parallel processing. Currently 4 partitions.

### CB Replica Assignment (range balancer, 4 replicas, 4 partitions)
With 4 replicas and 4 partitions each replica owns exactly 1 partition of highway-2 and repartition:
- **CB-1**: highway-2[0], repartition[0], watermarks[0]
- **CB-2**: highway-2[1], repartition[1], watermarks[0]
- **CB-3**: highway-2[2], repartition[2], watermarks[0]
- **CB-4**: highway-2[3], repartition[3], watermarks[0]

All 4 replicas subscribe to watermarks[0] (1 partition) so every replica sees every watermark update.

**Quix Cloud topology**: highway-2 has 4 partitions — confirmed by `highway-2[2]` offset error. The local test setup (`compose.local.yaml`) uses `deploy.replicas: 4` for both CB and CBNWM, starting 4 containers each.

---

## Key Bugs

### Bug 1: CB Never Fires `caught_up` Watermark Advance (EOS Stuck)

#### Root Cause
EOS (exactly-once semantics) produces **transaction control records** (EOS markers) at the end of each committed transaction. These markers are **invisible to `read_committed` consumers**: `consume()` returns 0 messages for them, but the consumer's Kafka-level position does **not** automatically advance past them in librdkafka.

Result: A replica's repartition partition ends up with:
- `position = N+1` (the EOS marker offset)
- `high_watermark = N+2` (LEO, including the EOS marker)

`_data_partitions_all_caught_up()` checks `pos < high` → always False → caught_up never fires → windows never expire → 0 output records.

### Detection Heuristic (`is_eos_stuck`)
A partition is EOS-stuck when **all four** are true:
1. `gap = hw - pos <= 10`: position is within 10 offsets of the high watermark (EOS markers are typically 1 offset from hw; this threshold filters out mid-stream false positives where hw is millions of offsets away).
2. `buffer.empty()`: all real data was consumed and processed.
3. `not buffer.paused`: the partition is NOT paused at the buffer level. A paused partition gets 0 from `consume()` regardless of EOS — `resume_empty()` handles the resume instead.
4. `consumer_position == _max_offset + 1` (position is right after the last data message — the only remaining records are invisible EOS markers).

### False Positive Risk (original version without guards 1+3)
Without `gap <= 10` and `not buffer.paused`, the heuristic triggers falsely when:
- A partition is **paused mid-processing** (buffer full at offset 5000, hw=2.7M). After the buffer drains, `consume()` returns 0 (paused), and `position == _max_offset+1` is trivially true. Without `not buffer.paused`, we'd seek to hw=2.7M and skip 2.695M messages.
- Confirmed by test results: Test 2 run 3 produced 330/600 (55%), Test 4 produced 90/200 (45%), both caused by premature seeks mid-stream.

---

### Bug 2: `UnboundLocalError: idle_watermark` (4-partition crash)

#### Root Cause
`_run_dataframe()` in `app.py` referenced `idle_watermark` at line 1025 (start of the else-branch body) before the variable was ever initialised. Python raises `UnboundLocalError` on the very first loop iteration, crashing all 4 CB replicas immediately.

This bug was latent — it only manifested when the double `if idle_watermark is not None:` block was introduced during debugging (second session), and the companion `idle_watermark = None` initialisation was forgotten.

With 2 partitions the tests appeared to pass because the CB containers crashed and were restarted (or because `state_manager.recovery_required` was True on the first iteration, deferring the bad branch). With 4 partitions and 4 active replicas the crash was reliably reproducible (CB=0 for all tests).

#### Fix
Add `idle_watermark = None` immediately before the `while run_tracker.running:` loop in `app.py`:

```python
idle_watermark = None          # ← new
while run_tracker.running:
    ...
    if idle_watermark is not None:   # line 1025 — now safe
        ...
    idle_watermark = watermark_manager.produce(caught_up=...)
    if idle_watermark is not None:   # line 1059
        ...
```

**Applied in**: `C:\repos\quix-streams_4a4\quixstreams\app.py` → wheel rebuilt → wheel copied to `croatian-burocrats/`.

---

### EOS Fix (Bug 1): `is_eos_stuck` + seek in `_feed_buffer()`

#### Fix (in `patch_buffering.py` + `patch_consumer.py`)
After each `_feed_buffer()` cycle, detect EOS-stuck partitions and **seek directly to the high watermark**. This advances the consumer position to `hw`, so `_data_partitions_all_caught_up()` sees `pos == high` → True.

**Critical detail — debounce required:** The seek must NOT fire immediately on first detection. CB replicas write to each other's repartition partitions (e.g., CB-3 processes highway-2-1 and produces to repartition-0, which is also CB-1's partition). While CB-1 is still committing its batch to repartition-0, CB-3 may see a temporary gap of 1 (an in-flight transaction looks like an EOS marker). Without a debounce, CB-3 would prematurely seek to the high watermark of repartition-0, skipping all of CB-1's uncommitted-yet data.

The debounce must exceed `commit_interval` (10 seconds in this project). **`_EOS_STABLE_SECONDS = 15.0`** ensures we only seek after the partition has been stably stuck for longer than the maximum inter-commit window.

```python
# patch_buffering.py — InternalConsumerBuffer.is_eos_stuck()
def is_eos_stuck(self, topic, partition, consumer_position, high_watermark):
    gap = high_watermark - consumer_position
    if gap <= 0 or gap > 10: return False
    group = self._partition_groups.get(partition)
    buffer = group._partition_buffers.get(topic) if group else None
    if buffer is None: return False
    return (buffer.empty() and not buffer.paused
            and buffer._max_offset >= 0
            and consumer_position == buffer._max_offset + 1)

# patch_consumer.py — _feed_buffer(), after self._buffer.feed():
_EOS_STABLE_SECONDS = 15.0  # must exceed commit_interval (10s) to avoid premature seeks
for tp in assignment:
    if self._topics[tp.topic].is_changelog: continue
    pos = consumer_positions.get((tp.topic, tp.partition))
    hw  = high_watermarks.get((tp.topic, tp.partition))
    key = (tp.topic, tp.partition)
    if pos is None or hw is None or pos >= hw:
        self._eos_stuck_since.pop(key, None)
        continue
    if self._buffer.is_eos_stuck(tp.topic, tp.partition, pos, hw):
        if key not in self._eos_stuck_since:
            self._eos_stuck_since[key] = monotonic()
        elif monotonic() - self._eos_stuck_since[key] >= _EOS_STABLE_SECONDS:
            self.seek(TopicPartition(topic=tp.topic, partition=tp.partition, offset=hw))
            self._eos_stuck_since.pop(key, None)
    else:
        self._eos_stuck_since.pop(key, None)
```

`_eos_stuck_since: dict[tuple[str, int], float]` is initialised in `InternalConsumer.__init__()`. The debounce timer resets whenever new data arrives (position advances, `pos >= hw` becomes True), so intermediate commits from other replicas correctly cancel a pending seek.

---

## Buffer System (`patch_buffering.py`)

### Classes
- `PartitionBuffer`: per-topic-partition ring buffer. Tracks `_max_offset`, `_high_watermark`, `_paused`, `non_blocking`.
- `PartitionBufferGroup`: groups buffers by partition number. Implements timestamp-ordered `pop()`.
- `InternalConsumerBuffer`: top-level; organises groups, exposes `feed()`, `pop()`, `pause_full()`, `resume_empty()`.

### Key Fields
- `_max_offset`: highest offset ever appended to this buffer (NOT reset on drain; used for EOS detection).
- `_high_watermark`: Kafka high watermark. **Reset to -1001 when the last message is popped** (so idleness becomes UNKNOWN until next feed cycle updates it).
- `non_blocking`: True for the watermarks topic partition buffer. Non-blocking buffers don't stall `pop()` when empty-but-active.

### Idleness States
| State | Condition |
|-------|-----------|
| UNKNOWN | `_high_watermark < 0` (reset after drain, or not yet set) |
| IDLE | `_max_offset + 1 >= _high_watermark` |
| ACTIVE | `_max_offset + 1 < _high_watermark` |

### `pop()` Blocking Logic
`PartitionBufferGroup.pop()` returns `None` (blocks) if ANY non-`non_blocking` buffer is empty AND not IDLE. This prevents out-of-order delivery when one partition has messages and another doesn't yet.

### `is_empty()` (for `buffer_empty` check)
Excludes `non_blocking` buffers — watermarks topic should not stall the caught-up check.

### `pause_full()` / `resume_empty()`
- Pause: buffer is full + ACTIVE + not already paused
- Resume: buffer is paused + (IDLE OR UNKNOWN OR not full)
- Single-partition groups are never paused (no need for balancing).

---

## Consumer System (`patch_consumer.py`)

### `_feed_buffer()` Flow
1. `consume(num_messages=10000, timeout=t)` — fetches from all non-paused partitions.
2. For each assigned non-changelog TP: `get_watermark_offsets(cached=True)` → `high_watermarks`.
3. `self.position(assignment)` → `consumer_positions`.
4. `self._buffer.feed(messages, high_watermarks, consumer_positions)`.
5. **EOS seek**: detect stuck partitions and seek to high watermark.
6. `resume_empty()` — resume paused partitions whose buffers are empty/IDLE.
7. `pause_full()` — pause partitions whose buffers are full and ACTIVE.

### `consumer_positions` (added fix)
Passed to `InternalConsumerBuffer.feed()` so `set_consumer_position()` can mark partitions IDLE when position ≥ high watermark (catching up normally), without relying solely on `_max_offset`.

---

## `_data_partitions_all_caught_up()` (in wheel, read-only)

```python
for tp in data_tps:  # non-changelog, non-watermarks
    _, high = self._consumer.get_watermark_offsets(tp, cached=False, timeout=2)
    pos = self._consumer.position([tp])[0].offset
    if pos < 0 or pos < high:
        return False
if not self._consumer.buffer_empty:
    return False
return True
```

- Rate-limited to once per second.
- Calls `get_watermark_offsets(cached=False)` — fresh fetch, not cached.
- Checks actual Kafka position, not buffer state.
- Also checks `buffer_empty` (calls `InternalConsumerBuffer.is_empty()`).

---

## WatermarkManager (`patch_watermarking.py`, read-only)

### Key Guards
- `_ever_stored`: set True only when `store()` is called (data processed). `produce(caught_up=True)` and `idle_advance` both require `_ever_stored=True`.
- `_owned_tps`: TPs whose watermarks have been stored. `_get_watermark()` returns `min` over owned TPs only.
- `_watermarks`: initialised with -1 for ALL partitions of ALL consumer topics.

---

## Test Structure (`run_test.py`)

| Test | Description | Expected Output |
|------|-------------|-----------------|
| Test 1 | Pre-existing data, CB/CBNWM started after; second TGSR flushes CBNWM | All windows from run 1 + 2 |
| Test 2 | 3 TGSR runs → start CB/CBNWM | 600 records total (CB); run_3 stuck (CBNWM) |
| Test 3 | CB/CBNWM running first, then 2 TGSR runs | 400 records |
| Test 4 | Continuous stream: CB+CBNWM running → TGCon started | Both producing; CB ~620 records in 60s, CBNWM ~880 |

### Key Config
- `WAIT_AFTER_START_CB = 240` seconds (Test 2; increased to handle 6M message backlog)
- `commit_every=1000, commit_interval=10` (CB config — debounce must exceed this)
- `max_partition_buffer_size=10000`
- `grace_ms=10s, tumbling_window=1s, .final()`

### Test Results (with 4 partitions / 4 replicas, wheel a8, 2026-03-23)
| Test | CB | CBNWM | Notes |
|------|----|-------|-------|
| 2 run_1 | 200 records, 1.87M (94%) | 200 records, 772K (39%) | All windows present; sum(count) shortfall is late-data timing |
| 2 run_2 | 200 records, 1.97M (99%) | 200 records, 285K (14%) | All windows present |
| 2 run_3 | PASS (200 records, 2M) | STUCK (0) — expected | Last run has no flush for CBNWM |
| 4 (continuous) | 620 records, 12.4K | 880 records, 17.6K | Both producing; CB fewer due to ~25s watermark warmup |

**Test 2 note**: CB emits all 600 window records (200 per run). sum(count) shortfall on run_1/2 is a timing issue — some repartition messages arrive after the watermark advance fires.

**Test 4 note**: ~25-second warmup before CB starts expiring windows. First ~3 watermark messages have per-TP fence = -1 (repartition TP watermark not yet received from Kafka). After warmup, windows expire in bursts aligned with EOS commits (~10s intervals). Caught-up advance flushes remaining windows after TGCon stops.

---

## EOS / Exactly-Once Kafka Notes

- **EOS markers** (transaction END_TXN records) appear at the end of each committed transaction. Under `read_committed` isolation, they are **filtered client-side** — not returned by `consume()`.
- **High watermark vs LSO**: `get_watermark_offsets()` in librdkafka returns the broker's LEO (Log End Offset), which **includes** EOS markers. This causes persistent LAG=1 in `rpk group describe` even after all real data is consumed.
- **Position stall**: After consuming all data messages, if the next offset is an EOS marker, librdkafka may NOT advance the position past it (especially if the partition is paused). This creates a permanent `pos < high` gap.
- **Multiple EOS markers**: If multiple transactions committed, there may be several EOS markers in a row. The `is_eos_stuck` check handles this: `position == _max_offset + 1` just needs to be true (any number of EOS markers between `_max_offset + 1` and `hw - 1`).

---

## Patch Files Summary

| File | Purpose |
|------|---------|
| `croatian-burocrats/patch_buffering.py` | Replaces `quixstreams` `InternalConsumerBuffer` + `PartitionBuffer` + `PartitionBufferGroup` |
| `croatian-burocrats/patch_consumer.py` | Replaces `quixstreams` `InternalConsumer` |
| `croatian-burocrats/patch_watermarking.py` | Replaces `quixstreams` `WatermarkManager` (read-only) |
| `croatian-burocrats/main.py` | CB application entry point |

All patches are baked into `quixstreams-4.0.0a8-py3-none-any.whl` in `croatian-burocrats/` for deployment (no runtime patching required). The patch files serve as reference copies.

---

## Change History (Key Fixes)

1. **`is_empty()` fix**: exclude `non_blocking` buffers from the emptiness check so watermarks topic doesn't stall the caught-up signal.
2. **`has_blocking_buffers` property + `pop()` rewrite**: try data groups (with blocking buffers) first; non-blocking-only groups tried last to prevent watermarks-only partitions from starving data processing.
3. **`consumer_positions` propagation**: pass `position()` results to buffer's `set_consumer_position()` so partitions at EOF can be marked IDLE even when buffer is empty.
4. **EOS seek fix**: detect partitions where position == `_max_offset + 1` < high watermark and seek to high watermark, bypassing invisible EOS control records.
5. **EOS debounce**: introduced `_eos_stuck_since` dict and `_EOS_STABLE_SECONDS = 15.0` to prevent premature seeks while another CB replica is still committing to the same repartition partition. Without this, Test 2 (3 pre-loaded runs) would produce only 45% of expected records (CB-1's repartition-0 windows never flushed because CB-3 seeked past it prematurely).
6. **Wheel packaging**: all patches baked into `quixstreams-4.0.0a8-py3-none-any.whl` in `croatian-burocrats/` for deployment (no runtime patching required).
7. **`idle_watermark` initialisation**: added `idle_watermark = None` before the `while` loop in `app.py` to fix `UnboundLocalError` crash on startup (manifested as CB=0 with 4 partitions and 4 replicas).
8. **Topology updated to 4 partitions/replicas**: `run_test.py` creates `highway-2` with 4 partitions; `compose.local.yaml` uses `deploy.replicas: 4` for both CB and CBNWM; watermarks topic remains 1 partition.
