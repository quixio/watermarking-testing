from quixstreams import Application
from quixstreams.sources import Source

import os
import random
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

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


MESSAGES_PER_BRAND_COLOUR = int(os.environ.get("MESSAGES_PER_BRAND_COLOUR", "1"))
RUN_ID_MIN_SECONDS = int(os.environ.get("RUN_ID_MIN_SECONDS", "20"))
RUN_ID_MAX_SECONDS = int(os.environ.get("RUN_ID_MAX_SECONDS", "200"))
IDLE_COLOUR_COUNT = int(os.environ.get("IDLE_COLOUR_COUNT", "3"))
IDLE_DURATION_SECONDS = int(os.environ.get("IDLE_DURATION_SECONDS", "30"))
ACTIVE_DURATION_SECONDS = int(os.environ.get("ACTIVE_DURATION_SECONDS", "60"))


class VehicleTrafficGenerator(Source):
    """
    Continuously generates vehicle traffic data in an infinite loop.
    Each second produces MESSAGES_PER_BRAND_COLOUR messages for every
    (brand, colour) combination, then sleeps the remainder of the second.
    """

    def _generate_plate(self):
        """Generate a unique numberplate-style key."""
        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        self._plate_counter += 1
        prefix = "".join(random.choices(letters, k=2))
        return f"{prefix}-{self._plate_counter:05d}"

    def _new_run_id(self):
        return "run_" + str(datetime.now(timezone.utc))

    def run(self):
        self._plate_counter = 0
        total_sent = 0
        second_offset = 0
        run_id = self._new_run_id()
        run_id_duration = random.randint(RUN_ID_MIN_SECONDS, RUN_ID_MAX_SECONDS)   # seconds before first rotation
        run_id_seconds_used = 0
        expected_per_second = len(BRANDS) * len(COLOURS) * MESSAGES_PER_BRAND_COLOUR
        print(f"Generating {MESSAGES_PER_BRAND_COLOUR} message(s) per (brand, colour) pair — {expected_per_second:,} messages/sec")
        print(f"run_id={run_id}, will rotate after {run_id_duration}s")

        idle_colours: set = set()
        idle_phase_start = time.monotonic()
        in_idle_phase = False
        phase_duration = ACTIVE_DURATION_SECONDS

        while self.running:
            tick_start = time.monotonic()

            # Rotate idle/active phase when current phase expires
            elapsed_in_phase = tick_start - idle_phase_start
            if elapsed_in_phase >= phase_duration:
                if in_idle_phase:
                    idle_colours = set()
                    in_idle_phase = False
                    phase_duration = ACTIVE_DURATION_SECONDS
                else:
                    idle_colours = set(random.sample(COLOURS, min(IDLE_COLOUR_COUNT, len(COLOURS))))
                    in_idle_phase = True
                    phase_duration = IDLE_DURATION_SECONDS
                idle_phase_start = tick_start
                print(f"Phase changed → idle={in_idle_phase}, suppressed colours: {idle_colours or 'none'}")

            second_start = datetime.now(timezone.utc).replace(microsecond=0)
            second_start_ms = int(second_start.timestamp() * 1000)
            second_sent = 0

            for colour in COLOURS:
                if colour in idle_colours:
                    continue
                for brand in BRANDS:
                    for _ in range(MESSAGES_PER_BRAND_COLOUR):
                        plate = self._generate_plate()
                        passengers = random.randint(1, 4)
                        ts_ms = second_start_ms + random.randint(0, 999)

                        value = {
                            "plate": plate,
                            "brand": brand,
                            "colour": colour,
                            "passengers": passengers,
                            "run_id": run_id,
                            "ts": ts_ms,
                        }

                        msg = self.serialize(key=brand, value=value)
                        self.produce(key=msg.key, value=msg.value)
                        second_sent += 1
                        total_sent += 1

            second_offset += 1
            run_id_seconds_used += 1
            elapsed = time.monotonic() - tick_start
            remaining = 1.0 - elapsed
            if remaining > 0:
                time.sleep(remaining)
            print(f"Produced second {second_offset} ({second_start.isoformat()}) — messages this second: {second_sent:,}, total sent: {total_sent:,}, run_id={run_id} ({run_id_seconds_used}/{run_id_duration}s), generation took: {elapsed:.3f}s")

            # Rotate run_id only after a full second-tick (all brands×colours complete)
            if run_id_seconds_used >= run_id_duration:
                run_id = self._new_run_id()
                run_id_duration = random.randint(RUN_ID_MIN_SECONDS, RUN_ID_MAX_SECONDS)
                run_id_seconds_used = 0
                print(f"run_id rotated → {run_id}, next rotation in {run_id_duration}s")

        print(f"Stopped. Total messages sent: {total_sent:,}")


def main():
    app = Application(consumer_group="traffic_generator", auto_create_topics=True)
    source = VehicleTrafficGenerator(name="vehicle-traffic-producer")
    output_topic = app.topic(name=os.environ["output"])

    app.add_source(source=source, topic=output_topic)
    app.run()


if __name__ == "__main__":
    main()
