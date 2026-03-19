# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
from datetime import timedelta
from quixstreams import Application

import os
import logging
import threading
import time
from collections import defaultdict
from quixstreams.dataframe.windows import Count, First

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()                                                                                                                                                                                                                  
                                                                                     
def main():
    # Use the message's own "ts" field (epoch ms) as the event timestamp.
    # This ensures windowing is driven by event time, not Kafka broker time.
    def ts_extractor(value, _headers, _timestamp, _timestamp_type) -> int:
        return value["ts"]

    # All replicas share the same consumer group so Kafka distributes
    # partitions between them automatically.
    app = Application(
        consumer_group="burocrats_watermarking_" + os.environ["consumer_group"],
        auto_create_topics=True,
        auto_offset_reset="earliest",
        processing_guarantee="exactly-once",
        commit_every=1000,
        commit_interval=10,
        watermarks_reset_on_start=False,
        watermarks_idle_partition_timeout=2.0,
        watermarks_idle_advance_after_ms=50_000,
    )

    input_topic = app.topic(
        name=os.environ["input"],
        timestamp_extractor=ts_extractor,
    )

    output_topic = app.topic(name=os.environ["output"])

    sdf = app.dataframe(topic=input_topic)

    sdf = sdf[sdf.contains("run_id")]

    # Loop 2: repartition by colour and compute per-second tumbling window counts.
    sdf = sdf.group_by("colour")

    # group_by repartitions via an internal Kafka topic and stamps messages with
    # broker time, losing the original event time.  Re-apply it from value["ts"]
    # (epoch ms, as produced) so the tumbling window uses event time correctly.
    sdf = sdf.set_timestamp(lambda row, *_: row["ts"])

    sdf = (
        sdf
        .tumbling_window(duration_ms=timedelta(seconds=1), grace_ms=timedelta(seconds=10))
        .agg(count=Count(), run_id=First("run_id"))
        .final()
    )

    sdf.print_table()
    
    sdf.to_topic(output_topic)

    # With our pipeline defined, now run the Application
    print("[STARTUP] calling app.run()", flush=True)  
    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()
