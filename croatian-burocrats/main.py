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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [colour-counter] %(message)s",
)
logger = logging.getLogger(__name__)

# -- Watermark debug instrumentation --------------------------------------                                                                                                                                                                                                                            
    import quixstreams.processing.watermarking as _wm_mod                                                                                                                                                                                                                                                     
    _wm_instance = [None]                                                                                                                                                                                                                                                                                    _orig_wm_set = _wm_mod.WatermarkManager.set_topics
    _orig_wm_recv = _wm_mod.WatermarkManager.receive                                                                                                                                                                                                                                                       
                                                                                                                                                                                                                                                                                                            def _dbg_set_topics(self, topics):
        _orig_wm_set(self, topics)                                                                                                                                                                                                                                                                         
        _wm_instance[0] = self
        logger.info(
            "[WM] Tracking %d TPs: %s",
            len(self._watermarks),                                                                                                                                                                                                                                                                                   ", ".join(f"{t}[{p}]" for (t, p) in sorted(self._watermarks.keys())),
        )                                                                                                                                                                                                                                                                                                     
    def _dbg_receive(self, message):                                                                                                                                                                                                                                                                             result = _orig_wm_recv(self, message)
        if result is not None:                                                                                                                                                                                                                                                                             
            logger.info("[WM] ADVANCED -> %d ms  (tp=%s[%d])", result, message["topic"], message["partition"])                                                                                                                                                                                             
        else:                                                                                                                                                                                                                                                                                                        stuck = [(t, p) for (t, p), v in self._watermarks.items() if v == -1]                                                                                                                                                                                                                          
            if stuck:                                                                                                                                                                                                                                                                                      
                logger.warning("[WM] STUCK at -1: %s", stuck) 
        return result
                                                                                                                                                                                                                                                                                                            _wm_mod.WatermarkManager.set_topics = _dbg_set_topics                                                                                                                                                                                                                                                    _wm_mod.WatermarkManager.receive    = _dbg_receive                                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                                                                                            
    _last_wm_dump = [0.0]
    def _wm_dump(value):
        now = time.monotonic()                                                                                                                                                                                                                                                                             
        if now - _last_wm_dump[0] >= 30.0:                                                                                                                                                                                                                                                                           _last_wm_dump[0] = now                                                                                                                                                                                                                                                                         
            if _wm_instance[0] is not None:                                                                                                                                                                                                                                                                
                rows = sorted(_wm_instance[0]._watermarks.items())                                                                                                                                                                                                                                                       logger.info(
                    "[WM] dump:\n%s",                                                                                                                                                                                                                                                                      
                    "\n".join(
                        f"  {'STUCK' if v == -1 else '     '} {t}[{p}] = {v}"
                        for (t, p), v in rows                                                                                                                                                                                                                                                                                ),
                )                                                                                                                                                                                                                                                                                          
        return value
# -- end instrumentation --------------------------------------------------                                                                                                                                                                                                                         
                                                                                     

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
        commit_interval=10
    )

    input_topic = app.topic(
        name=os.environ["input"],
        timestamp_extractor=ts_extractor,
    )

    output_topic = app.topic(name=os.environ["output"])

    sdf = app.dataframe(topic=input_topic)

    sdf = sdf[sdf.contains("run_id")]

    sdf = sdf.apply(_wm_dump)  

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
    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()
