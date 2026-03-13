from quixstreams import Application
from quixstreams.sources import Source

import os
import random
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


class VehicleTrafficGenerator(Source):
    """
    Continuously generates vehicle traffic data in an infinite loop.
    Each discrete second has exactly 10,000 vehicles per colour (200,000 vehicles/sec).
    Runs until stopped.
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

        while self.running:
            second_start = datetime.now(timezone.utc).replace(microsecond=0)
            second_start_ms = int(second_start.timestamp() * 1000)
            second_sent = 0

            for colour in COLOURS:
                for _ in range(10_000):
                    brand = random.choice(BRANDS)
                    plate = self._generate_plate()
                    passengers = random.randint(1, 4)
                    ts_ms = second_start_ms + random.randint(0, 999)

                    value = {
                        "plate": plate,
                        "brand": brand,
                        "colour": colour,
                        "passengers": passengers,
                        "ts": ts_ms,
                    }

                    msg = self.serialize(key=brand, value=value)
                    self.produce(key=msg.key, value=msg.value)
                    second_sent += 1
                    total_sent += 1

            second_offset += 1
            print(f"Produced second {second_offset} ({second_start.isoformat()}) — messages this second: {second_sent:,}, total sent: {total_sent:,}")

        print(f"Stopped. Total messages sent: {total_sent:,}")


def main():
    app = Application(consumer_group="traffic_generator", auto_create_topics=True)
    source = VehicleTrafficGenerator(name="vehicle-traffic-producer")
    output_topic = app.topic(name=os.environ["output"])

    app.add_source(source=source, topic=output_topic)
    app.run()


if __name__ == "__main__":
    main()
