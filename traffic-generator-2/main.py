from quixstreams import Application
from quixstreams.sources import Source

import os
import random
import time
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
load_dotenv()

CET = timezone(timedelta(hours=1))

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
RUN_DURATION_SECONDS = int(os.environ.get("RUN_DURATION_SECONDS", "60"))


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

    def run(self):
        self._plate_counter = 0
        total_sent = 0
        second_offset = 0
        run_id = "run_" + str(datetime.now(CET))
        expected_per_second = len(BRANDS) * len(COLOURS) * MESSAGES_PER_BRAND_COLOUR
        print(f"Generating {MESSAGES_PER_BRAND_COLOUR} message(s) per (brand, colour) pair — {expected_per_second:,} messages/sec")
        print(f"run_id={run_id}")
        print(f"Will run for {RUN_DURATION_SECONDS}s then stop.")

        while self.running and second_offset < RUN_DURATION_SECONDS:
            tick_start = time.monotonic()
            second_start = datetime.now(CET).replace(microsecond=0)
            second_start_ms = int(second_start.timestamp() * 1000)
            second_sent = 0

            for brand in BRANDS:
                for colour in COLOURS:
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
            elapsed = time.monotonic() - tick_start
            remaining = 1.0 - elapsed
            if remaining > 0:
                time.sleep(remaining)
            print(f"Produced second {second_offset}/{RUN_DURATION_SECONDS} ({second_start.isoformat()}) — messages this second: {second_sent:,}, total sent: {total_sent:,}, run_id={run_id}, generation took: {elapsed:.3f}s")

        print(f"Stopped. Total messages sent: {total_sent:,}")


def main():
    app = Application(consumer_group="traffic_generator", auto_create_topics=True)
    source = VehicleTrafficGenerator(name="vehicle-traffic-producer")
    output_topic = app.topic(name=os.environ["output"])

    app.add_source(source=source, topic=output_topic)
    app.run()


if __name__ == "__main__":
    main()
