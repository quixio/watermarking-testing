# Croatian Burocrats (CB)

Quix Streams windowed aggregation service with event-time watermarking.

## What It Does

1. Consumes from `highway-2` (4 partitions), filters for messages containing `run_id`.
2. Repartitions by `colour` via `group_by("colour")` into an internal repartition topic.
3. Re-applies original event time from `value["ts"]` (broker time is lost during repartition).
4. Runs a **1-second tumbling window** with **10-second grace period** counting vehicles per colour.
5. On each window expiry (`final()`), publishes `{run_id, count, start, end}` to the output topic.

## Key Configuration

```python
app = Application(
    consumer_group="burocrats_watermarking_" + os.environ["consumer_group"],
    processing_guarantee="exactly-once",
    commit_every=1000,
    max_partition_buffer_size=10000,
    commit_interval=10,
    eos_stable_seconds=15.0,           # debounce for EOS seek (must > commit_interval)
    watermarks_idle_partition_timeout=30.0,  # exclude stale -1 partitions from global min
    watermarks_idle_advance_after_ms=30000,  # force-advance watermark after 30s idle
)
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `input` | Input topic (e.g. `highway-2`) |
| `output` | Output topic (e.g. `colours`) |
| `consumer_group` | Consumer group suffix (e.g. `v1`) |

## Deployment

- **4 replicas** (one per partition of `highway-2`)
- Uses patched quixstreams 4.0.0a7 from `quixstreams-4.0.0a7-py3-none-any.whl`
- Wheel is built from `C:\repos\quix-streams_4a4` with all EOS and watermarking fixes

## Patch Files

The `patch_*.py` files are **reference copies** of the patched quixstreams source modules.
They are NOT used at runtime (the wheel contains the patches). They exist for version control
and documentation of what was changed.

| Patch File | Patched Module |
|-----------|----------------|
| `patch_app.py` | `quixstreams/app.py` |
| `patch_consumer.py` | `quixstreams/internal_consumer/consumer.py` |
| `patch_buffering.py` | `quixstreams/internal_consumer/buffering.py` |
| `patch_manager.py` | `quixstreams/models/topics/manager.py` |
| `patch_watermarking.py` | `quixstreams/processing/watermarking.py` |

## Key Fixes in Patched Wheel

1. **EOS seek** (`consumer.py`, `buffering.py`): detects partitions stuck on invisible EOS transaction control records and seeks past them after a 15-second debounce.
2. **Non-blocking watermarks buffer** (`buffering.py`): watermarks topic partition never stalls data flow.
3. **`idle_watermark` initialisation** (`app.py`): fixed `UnboundLocalError` crash on startup.
4. **Watermarks topic 1 partition** (`manager.py`): all replicas share one watermarks partition.
5. **Idle advance + partition timeout** (`watermarking.py`): force-advance watermark after 30s idle; exclude stalled -1 partitions from global min.

See [../KNOWLEDGEBASE.md](../KNOWLEDGEBASE.md) for full debugging documentation.
