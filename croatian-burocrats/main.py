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

    sdf = app.dataframe(topic=input_topic)

    # Repartition by colour – each colour lands on a dedicated partition
    # so windowed state per colour is kept in a single replica.
    sdf = sdf.group_by("colour")

    # 4-second tumbling window: count how many vehicles of each colour
    # are seen inside every non-overlapping 4-second bucket.
    # The reducer carries both the colour name and the running count so
    # the log line has everything it needs without touching the Kafka key.
    sdf = (
        sdf
        .tumbling_window(duration_ms=timedelta(seconds=4))
        .reduce(
            initializer=lambda row: {"colour": row["colour"], "count": 1},
            reducer=lambda agg, row: {"colour": agg["colour"], "count": agg["count"] + 1},
        )
        .final()   # emit exactly once when the window closes and resets
    )

    # Log one line per closed window: colour, count, and the time frame.
    def log_window(result):
        colour = result["value"]["colour"]
        count  = result["value"]["count"]
        start  = result["start"]   # epoch ms
        end    = result["end"]     # epoch ms
        logger.info(
            "colour=%-12s  count=%4d  window=[%d – %d]",
            colour, count, start, end,
        )
        return result

    sdf = sdf.apply(log_window)

    # With our pipeline defined, now run the Application
    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()
