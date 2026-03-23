# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
from datetime import timedelta
from pathlib import Path
from quixstreams import Application

import os
import logging
import shutil
import threading
import time
from collections import defaultdict
from quixstreams.dataframe.windows import Count, First

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()


def clear_state_if_requested():
    """
    Delete ALL contents of /app/state/ so that latest_expired_window_end
    resets to 0 and replayed data is not classified as "late".

    Wipes the entire state directory rather than guessing the consumer group
    subdirectory name (which Quix Cloud prefixes with the environment name).

    Controlled by CLEAR_STATE env var:
      "true"  — always clear state on startup
      "false" / unset — keep state (default)
    """
    if os.environ.get("CLEAR_STATE", "").lower() != "true":
        return

    state_dir = Path("/app/state")
    if state_dir.exists():
        entries = list(state_dir.iterdir())
        if entries:
            print(f"[STARTUP] CLEAR_STATE=true — deleting {len(entries)} entries in {state_dir}: "
                  f"{[e.name for e in entries]}", flush=True)
            for entry in entries:
                if entry.is_dir():
                    shutil.rmtree(entry)
                else:
                    entry.unlink()
        else:
            print(f"[STARTUP] CLEAR_STATE=true — state directory empty: {state_dir}", flush=True)
    else:
        print(f"[STARTUP] CLEAR_STATE=true — no state directory: {state_dir}", flush=True)


def main():
    # Use the message's own "ts" field (epoch ms) as the event timestamp.
    # This ensures windowing is driven by event time, not Kafka broker time.
    def ts_extractor(value, _headers, _timestamp, _timestamp_type) -> int:
        return value["ts"]

    consumer_group = "burocrats_watermarking_" + os.environ["consumer_group"]
    clear_state_if_requested()

    # All replicas share the same consumer group so Kafka distributes
    # partitions between them automatically.
    app = Application(
        consumer_group=consumer_group,
        auto_create_topics=True,
        auto_offset_reset="earliest",
        processing_guarantee="exactly-once",
        commit_every=1000,
        max_partition_buffer_size=10000,
        commit_interval=10,
        eos_stable_seconds=15.0,
        watermarks_idle_partition_timeout=30.0,
        watermarks_idle_advance_after_ms=30000,
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
