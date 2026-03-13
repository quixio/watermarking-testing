# Watermarking Test

A Quix Streams pipeline for testing event-time watermarking with simulated vehicle traffic data.

## Pipeline

```
Traffic Generator → Croatian Burocrats → Normalization
     (highway-2)         (colours)        (flat-colours)
```

### Traffic Generator
Quix Streams **Source** (Job). Generates 12,000 simulated vehicle messages over 60 seconds.

- 20 colours × 10 vehicles × 60 seconds
- Each message contains: `plate`, `brand`, `colour`, `passengers`, `ts_start`, `ts` (epoch ms)
- Message key is the vehicle brand
- Logs progress per second: messages sent that second + running total

**Environment variables:**
| Variable | Type | Description |
|----------|------|-------------|
| `output` | OutputTopic | Topic to publish vehicle messages to (e.g. `highway-2`) |

---

### Croatian Burocrats
Quix Streams **Service** (4 replicas). Consumes raw vehicle messages, counts vehicles per colour using 1-second tumbling windows, and publishes window results.

Two independent processing loops:
1. **Raw counter** — counts every incoming message and tracks cumulative per-colour totals
2. **Windowed counter** — repartitions by colour, applies 1s tumbling windows with event-time watermarking, emits final window counts

After 15 seconds of inactivity both loops report to log:
- Total messages received
- Per-colour cumulative counts + grand total + number of unique colours seen

**Environment variables:**
| Variable | Type | Description |
|----------|------|-------------|
| `input` | InputTopic | Topic to consume from (e.g. `highway-2`) |
| `output` | OutputTopic | Topic to publish window results to (e.g. `colours`) |

---

### Normalization
Quix Streams **Service**. Flattens windowed colour-count messages from `colours` into a single-level dict and forwards them to `flat-colours`.

After 15 seconds of inactivity reports to log:
- Per-colour totals forwarded (summed from windowed `count` values)
- Grand total

**Environment variables:**
| Variable | Type | Description |
|----------|------|-------------|
| `output` | OutputTopic | Topic to publish flattened rows to (e.g. `flat-colours`) |

---

## Topics

| Topic | Partitions | Description |
|-------|-----------|-------------|
| `highway-2` | 4 | Raw vehicle messages from Traffic Generator |
| `colours` | 4 | Per-colour 1s window counts from Croatian Burocrats |
| `flat-colours` | — | Flattened window rows from Normalization |

## Running locally

Each service requires a `.env` file with the relevant environment variables and a running Kafka broker configured via the Quix Streams connection settings.
