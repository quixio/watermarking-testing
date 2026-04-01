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

# Total messages per 10s window = 20 brands * 20 colours * MESSAGES_PER_BRAND_COLOUR = 400 * N
MESSAGES_PER_BRAND_COLOUR = int(os.environ.get("MESSAGES_PER_BRAND_COLOUR", "1"))
# How many 10-second event-time windows to generate
NUM_WINDOWS = int(os.environ.get("NUM_WINDOWS", "6"))
# Real-time seconds to spread each window's messages over
SEND_DURATION_SECONDS = float(os.environ.get("SEND_DURATION_SECONDS", "10"))
WINDOW_SECONDS = int(os.environ.get("WINDOW_SECONDS", "10"))
RUN_ID_PREFIX = os.environ.get("RUN_ID_PREFIX", "run")


class VehicleTrafficGeneratorV2(Source):
    """
    Generates vehicle traffic data in sequential 10-second event-time windows.
    Within each window, timestamps are uniformly random across the full 10s range,
    ensuring all brand*colour combinations are evenly represented.
    Messages are sent over SEND_DURATION_SECONDS real time per window.
    """

    def _generate_plate(self):
        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        self._plate_counter += 1
        prefix = "".join(random.choices(letters, k=2))
        return f"{prefix}-{self._plate_counter:05d}"

    def run(self):
        self._plate_counter = 0
        total_sent = 0
        run_id = "run_" + str(datetime.now(CEST)) + "_" + RUN_ID_PREFIX

        messages_per_window = len(BRANDS) * len(COLOURS) * MESSAGES_PER_BRAND_COLOUR
        window_start = datetime.now(CEST).replace(microsecond=0)
        window_start_ms = int(window_start.timestamp() * 1000)

        window_ms = WINDOW_SECONDS * 1000

        print(f"TGSR-V2: {MESSAGES_PER_BRAND_COLOUR} msg/brand/colour, "
              f"{messages_per_window:,} msgs/window, "
              f"{NUM_WINDOWS} windows ({WINDOW_SECONDS}s each), "
              f"{SEND_DURATION_SECONDS}s send duration per window")
        print(f"run_id={run_id}")

        for window_idx in range(NUM_WINDOWS):
            if not self.running:
                break

            ws_ms = window_start_ms + window_idx * window_ms
            tick_start = time.monotonic()

            # Build all messages for this window
            messages = []
            for brand in BRANDS:
                for colour in COLOURS:
                    for _ in range(MESSAGES_PER_BRAND_COLOUR):
                        plate = self._generate_plate()
                        passengers = random.randint(1, 4)
                        ts_ms = ws_ms + random.randint(0, window_ms - 1)

                        value = {
                            "plate": plate,
                            "brand": brand,
                            "colour": colour,
                            "passengers": passengers,
                            "run_id": run_id,
                            "ts": ts_ms,
                        }
                        messages.append((brand, value))

            # Shuffle so messages arrive in random order, not grouped by brand/colour
            random.shuffle(messages)

            # Send all messages
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
                  f"sent {len(messages):,} msgs, "
                  f"event time [{datetime.fromtimestamp(ws_ms / 1000, tz=CEST).strftime('%H:%M:%S')}"
                  f"..{datetime.fromtimestamp((ws_ms + window_ms) / 1000, tz=CEST).strftime('%H:%M:%S')}), "
                  f"took {elapsed_total:.3f}s (gen+send {elapsed:.3f}s), "
                  f"run_id={run_id}")

        print(f"Done. Total messages sent: {total_sent:,}")


def main():
    app = Application(consumer_group="traffic_generator_v2", auto_create_topics=True)
    source = VehicleTrafficGeneratorV2(name="vehicle-traffic-producer-v2")
    output_topic = app.topic(name=os.environ["output"])

    app.add_source(source=source, topic=output_topic)
    app.run()


if __name__ == "__main__":
    main()
