# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
from datetime import timedelta
from quixstreams import Application

import os
import logging

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [colour-counter] %(message)s",
)
logger = logging.getLogger(__name__)


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

    sdf = app.dataframe(topic=input_topic)

    # Force all messages onto a single partition by grouping on a constant key.
    # This guarantees exactly one window state across all replicas, so .final()
    # produces exactly one message per window.
    sdf["_all"] = "all"
    sdf = sdf.group_by("_all")

    sdf = (
        sdf
        .tumbling_window(duration_ms=timedelta(seconds=1), grace_ms=timedelta(seconds=1))
        .reduce(
            initializer=lambda row: {row["colour"]: 1},
            reducer=lambda agg, row: {**agg, row["colour"]: agg.get(row["colour"], 0) + 1},
        )
        .final()
    )

    def log_window(result):
        counts = result["value"]
        start  = result["start"]
        end    = result["end"]
        for colour, count in sorted(counts.items()):
            logger.info("colour=%-12s  count=%4d  window=[%d – %d]", colour, count, start, end)
        return result

    sdf.apply(log_window).to_topic(output_topic)

    # With our pipeline defined, now run the Application
    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()
