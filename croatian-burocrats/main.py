# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
from datetime import timedelta
from quixstreams import Application

import os
import logging
import threading
import time
from collections import defaultdict

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [colour-counter] %(message)s",
)
logger = logging.getLogger(__name__)

# --- Shared state for independent counters ---
_lock = threading.Lock()
_total_received = 0
_colour_counts = defaultdict(int)
_last_message_time = 0.0


def _inactivity_monitor():
    """
    Background thread: prints total message count and per-colour totals
    once when no new message has arrived for 15 seconds.
    """
    last_reported_at = 0.0
    while True:
        time.sleep(1)
        with _lock:
            lmt = _last_message_time
            total = _total_received
            counts = dict(_colour_counts)

        if lmt > 0 and (time.time() - lmt) >= 15 and lmt != last_reported_at:
            logger.info("No new messages for 15s — total messages received: %d", total)
            if counts:
                logger.info("Per-colour totals (cumulative):")
                for colour, count in sorted(counts.items()):
                    logger.info("  %-12s : %d", colour, count)
                logger.info("  %-12s : %d", "GRAND TOTAL", sum(counts.values()))
                logger.info("  %-12s : %d", "UNIQUE COLOURS", len(counts))
            last_reported_at = lmt


def _track_message(row):
    """
    Loop 1 — counts every raw message received and tracks per-colour totals.
    Runs before any repartitioning so it sees all messages exactly once.
    """
    global _total_received, _last_message_time
    with _lock:
        _total_received += 1
        _last_message_time = time.time()
        if "colour" in row:
            _colour_counts[row["colour"]] += 1
    return row


def main():
    # Use the message's own "ts" field (epoch ms) as the event timestamp.
    # This ensures windowing is driven by event time, not Kafka broker time.
    def ts_extractor(value, _headers, _timestamp, _timestamp_type) -> int:
        return value["ts"]

    # All replicas share the same consumer group so Kafka distributes
    # partitions between them automatically.
    app = Application(
        consumer_group="colour_counter_v1",
        auto_create_topics=True,
        auto_offset_reset="earliest",
    )

    input_topic = app.topic(
        name=os.environ["input"],
        timestamp_extractor=ts_extractor,
    )

    output_topic = app.topic(name="colours")

    # Start the inactivity monitor in the background.
    threading.Thread(target=_inactivity_monitor, daemon=True).start()

    sdf = app.dataframe(topic=input_topic)

    # Loop 1: count every raw message and track per-colour totals (side-effect).
    sdf = sdf.apply(_track_message)

    # Loop 2: repartition by colour and compute per-second tumbling window counts.
    sdf = sdf.group_by("colour")

    # group_by repartitions via an internal Kafka topic and stamps messages with
    # broker time, losing the original event time.  Re-apply it from value["ts"]
    # (epoch ms, as produced) so the tumbling window uses event time correctly.
    sdf = sdf.set_timestamp(lambda row, *_: row["ts"])

    sdf = (
        sdf
        .tumbling_window(duration_ms=timedelta(seconds=1), grace_ms=timedelta(seconds=1))
        .reduce(
            initializer=lambda row: {"colour": row["colour"], "count": 1},
            reducer=lambda agg, _: {**agg, "count": agg["count"] + 1},
        )
        .final()
    )

    def log_window(result):
        colour = result["value"]["colour"]
        count  = result["value"]["count"]
        start  = result["start"]
        end    = result["end"]
        logger.info("colour=%-12s  count=%4d  window=[%d – %d]", colour, count, start, end)
        return result

    sdf.apply(log_window).to_topic(output_topic)

    # With our pipeline defined, now run the Application
    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()
