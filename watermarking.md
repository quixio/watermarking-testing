# Watermarking Feature (PR #1060)

Branch: `feature/watermarking` → `main`

## What it does

Distributed, Kafka-backed watermarking for time-windowed stream processing.
Global watermark = `min(all partition watermarks)` — signals "all data up to this point has arrived."
Enables windows to expire by time across all keys, not just when a new message arrives for the same key.

## Key Files

- `quixstreams/processing/watermarking.py` — `WatermarkMessage` TypedDict + `WatermarkManager` class
- `quixstreams/models/topics/manager.py` — `watermarks_topic()` creates internal Kafka topic `<consumer_group>.watermarks`
- `quixstreams/app.py` — integrates WatermarkManager into the run loop
- `quixstreams/dataframe/windows/time_based.py` — `expire_by_partition()`, before/after update callbacks
- `quixstreams/core/stream/stream.py` — `is_watermark` flag threading, sink blocking

## How it flows

1. Every normal message → `watermark_manager.store(topic, partition, timestamp, default=True)`
2. On idle loop → `watermark_manager.produce()` flushes buffered timestamps to the internal watermarks Kafka topic (rate-limited by `watermarking_interval`)
3. All instances consume the watermarks topic → `watermark_manager.receive()` → global watermark = `min(all TPs)`
4. When global watermark advances → re-run pipeline for each assigned partition with `value=None, key=None, timestamp=<watermark>, is_watermark=True`
5. `apply`/`filter`/`update` ops → no-op, pass watermark downstream
6. `transform` with `on_watermark` → handler fires, emits real records; watermark continues downstream
7. Sink boundary → watermark signal dropped (never reaches sinks)

## Window expiry

- `TimeWindow.final()` / `.current()` register `on_watermark` callbacks
- Watermark arrival triggers `expire_by_partition(transaction, timestamp_ms)` — sweeps ALL keys in the partition's state store
- Closes any window with `end_time ≤ watermark − grace`
- Major improvement: windows close even if no new messages arrive for a key

## New Application Parameters

| Parameter | Default | Description |
|---|---|---|
| `watermarking_default_assignor_enabled` | `True` | Auto-track Kafka message timestamps as watermarks |
| `watermarking_interval` | `1.0` | Seconds between watermark flushes to Kafka |
| `broker_availability_timeout` | `120.0` | Crash if Kafka unreachable (triggers orchestrator restart) |

## New TimeWindow Callbacks

- `before_update(current_value, new_value, key, timestamp_ms, headers) -> bool` — return True to emit window BEFORE new value is added
- `after_update(new_aggregated, new_value, key, timestamp_ms, headers) -> bool` — return True to emit window AFTER update

## Other API changes

- `StreamingDataFrame.set_timestamp(fn)` — now also stores a non-default watermark (overrides auto Kafka-timestamp watermark)
- `StreamingDataFrame.test(..., is_watermark=False)` — test watermark code paths
- `processed_offsets` recovery mechanism removed (replaced by watermark-driven approach)
- Watermarks exported from `quixstreams/__init__.py`

## Design Properties

- Global watermark gated by slowest partition — no incorrect early window closure
- Watermarks stored in Kafka — shared across all consumer group instances
- Watermarks transparent to non-windowed pipeline ops
- Watermarks blocked from reaching sinks (`_sink_wrapper` drops them)
