"""
TGSR-V3: Burst traffic generator designed to stress-test watermarking.

Sends large bursts of data per window with minimal pacing, creating natural
processing speed differences between CB replicas. When a fast replica produces
window W+1 data to the repartition topic before a slow replica finishes
window W, CBNWM drops the late-arriving window W data while CB (watermarks)
preserves it via the global watermark.

Key parameters:
  MESSAGES_PER_BRAND_COLOUR: Higher = more data per window = bigger processing lag
  SEND_DURATION_SECONDS: Lower = faster bursts = more pronounced timing differences
"""
from quixstreams import Application
from quixstreams.sources import Source

import os
import random
import time
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
load_dotenv()

CEST = timezone(timedelta(hours=2))

BRANDS = [
    "Toyota", "Honda", "Ford", "BMW", "Mercedes",
    "Audi", "Volkswagen", "Tesla", "Hyundai", "Kia",
    "Nissan", "Chevrolet", "Mazda", "Subaru", "Volvo",
    "Porsche", "Lexus", "Jaguar", "Ferrari", "Lamborghini",
]

COLOURS = [
    "Red", "Blue", "Green", "Black", "White",
    "Silver", "Grey", "Yellow", "Orange", "Purple",
    "Brown", "Beige", "Gold", "Navy", "Teal",
    "Maroon", "Coral", "Ivory", "Cyan", "Magenta",
]

MESSAGES_PER_BRAND_COLOUR = int(os.environ.get("MESSAGES_PER_BRAND_COLOUR", "50"))
NUM_WINDOWS = int(os.environ.get("NUM_WINDOWS", "6"))
SEND_DURATION_SECONDS = float(os.environ.get("SEND_DURATION_SECONDS", "1"))
RUN_ID_PREFIX = os.environ.get("RUN_ID_PREFIX", "burst")


class BurstTrafficGenerator(Source):
    """
    Generates traffic in fast bursts to create replica processing lag.
    Each window's messages are sent as quickly as possible, with only
    SEND_DURATION_SECONDS minimum gap between windows.
    """

    def _generate_plate(self):
        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        self._plate_counter += 1
        prefix = "".join(random.choices(letters, k=2))
        return f"{prefix}-{self._plate_counter:05d}"

    def run(self):
        self._plate_counter = 0
        total_sent = 0
        run_id = str(datetime.now(CEST)) + "_" + RUN_ID_PREFIX

        messages_per_window = len(BRANDS) * len(COLOURS) * MESSAGES_PER_BRAND_COLOUR
        window_start = datetime.now(CEST).replace(microsecond=0)
        window_start_ms = int(window_start.timestamp() * 1000)

        print(f"TGSR-V3 BURST: {MESSAGES_PER_BRAND_COLOUR} msg/brand/colour, "
              f"{messages_per_window:,} msgs/window, "
              f"{NUM_WINDOWS} windows, "
              f"{SEND_DURATION_SECONDS}s min gap between windows")
        print(f"run_id={run_id}")

        for window_idx in range(NUM_WINDOWS):
            if not self.running:
                break

            ws_ms = window_start_ms + window_idx * 10_000
            tick_start = time.monotonic()

            # Build all messages for this window — NOT shuffled by brand
            # so that messages for the same brand (same highway-2 partition)
            # arrive together, maximizing per-partition batch processing
            messages = []
            for brand in BRANDS:
                for colour in COLOURS:
                    for _ in range(MESSAGES_PER_BRAND_COLOUR):
                        plate = self._generate_plate()
                        passengers = random.randint(1, 4)
                        ts_ms = ws_ms + random.randint(0, 9999)

                        value = {
                            "plate": plate,
                            "brand": brand,
                            "colour": colour,
                            "passengers": passengers,
                            "run_id": run_id,
                            "ts": ts_ms,
                        }
                        messages.append((brand, value))

            # Send all messages as fast as possible (no per-message delay)
            for key, value in messages:
                if not self.running:
                    break
                msg = self.serialize(key=key, value=value)
                self.produce(key=msg.key, value=msg.value)
                total_sent += 1

            elapsed = time.monotonic() - tick_start
            remaining = SEND_DURATION_SECONDS - elapsed
            if remaining > 0:
                time.sleep(remaining)
            elapsed_total = time.monotonic() - tick_start

            print(f"Window {window_idx + 1}/{NUM_WINDOWS}: "
                  f"sent {len(messages):,} msgs in {elapsed:.3f}s, "
                  f"event time [{datetime.fromtimestamp(ws_ms / 1000, tz=CEST).strftime('%H:%M:%S')}"
                  f"..{datetime.fromtimestamp((ws_ms + 10_000) / 1000, tz=CEST).strftime('%H:%M:%S')}), "
                  f"run_id={run_id}")

        print(f"Done. Total messages sent: {total_sent:,}")


def main():
    app = Application(consumer_group="traffic_generator_v3", auto_create_topics=True)
    source = BurstTrafficGenerator(name="vehicle-traffic-burst")
    output_topic = app.topic(name=os.environ["output"])

    app.add_source(source=source, topic=output_topic)
    app.run()


if __name__ == "__main__":
    main()
