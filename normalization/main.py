# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
from quixstreams import Application
from quixstreams.dataframe import StreamingDataFrame

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
    format="%(asctime)s [normalization] %(message)s",
)
logger = logging.getLogger(__name__)

# --- Shared state for colour counter ---
_lock = threading.Lock()
_colour_counts = defaultdict(int)
_last_message_time = 0.0


def _inactivity_monitor():
    """Prints per-colour totals once when no new message has arrived for 15 seconds."""
    last_reported_at = 0.0
    while True:
        time.sleep(1)
        with _lock:
            lmt = _last_message_time
            counts = dict(_colour_counts)

        if lmt > 0 and (time.time() - lmt) >= 15 and lmt != last_reported_at:
            logger.info("No new messages for 15s — colour totals forwarded:")
            for colour, count in sorted(counts.items()):
                logger.info("  %-12s : %d", colour, count)
            logger.info("  %-12s : %d", "GRAND TOTAL", sum(counts.values()))
            last_reported_at = lmt


def _track_colour(row: dict) -> dict:
    """Side-effect: track per-colour counts from flattened rows."""
    global _last_message_time
    colour = row.get("colour")
    count = row.get("count", 1)
    with _lock:
        _last_message_time = time.time()
        if colour:
            _colour_counts[colour] += count
    return row


def flatten_colours(row: dict) -> dict:
    """Flatten the windowed colour counts into a single row using 'end' as timestamp."""
    flat = {"time": row["end"]}
    flat.update(row["value"])
    return flat


def define_pipeline(sdf: StreamingDataFrame):

    sdf = sdf.apply(flatten_colours)

    # Set row timestamp from the 'end' field (milliseconds → nanoseconds for Quix)
    sdf = sdf.set_timestamp(lambda row, *_: row["time"])

    # Track colour counts for inactivity reporting
    sdf = sdf.apply(_track_colour)

    # Optional printing for debugging.
    #sdf = sdf.print(metadata=True)

    return sdf


def main():

    # Setup necessary objects
    app = Application(
        consumer_group="my_transformation",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    input_topic = app.topic(name="colours")
    output_topic = app.topic(name=os.environ["output"])
    sdf = app.dataframe(topic=input_topic)

    # Start the inactivity monitor in the background
    threading.Thread(target=_inactivity_monitor, daemon=True).start()

    # Apply the pipeline transformations
    sdf = define_pipeline(sdf)

    # Finish off by writing to the final result to the output topic
    sdf.to_topic(output_topic)

    # With our pipeline defined, now run the Application
    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()
