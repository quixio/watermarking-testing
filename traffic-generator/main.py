from quixstreams import Application
from quixstreams.sources import Source

import os
import random
from datetime import datetime, timezone, timedelta

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
    Generates 1 hour of vehicle traffic data.
    Each discrete minute has exactly 10 vehicles per colour (200 vehicles/min).
    Total: 20 colours × 10 vehicles × 60 minutes = 12,000 messages.
    """

    def _generate_plate(self):
        """Generate a unique numberplate-style key."""
        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        self._plate_counter += 1
        prefix = "".join(random.choices(letters, k=2))
        return f"{prefix}-{self._plate_counter:05d}"

    def run(self):
        self._plate_counter = 0
        ts_start = datetime.now(timezone.utc)
        base_minute = ts_start.replace(second=0, microsecond=0)

        for minute_offset in range(60):
            ts = base_minute + timedelta(minutes=minute_offset)
            ts_ms = int(ts.timestamp() * 1000)

            for colour in COLOURS:
                for _ in range(10):
                    brand = random.choice(BRANDS)
                    plate = self._generate_plate()
                    passengers = random.randint(1, 4)

                    value = {
                        "plate": plate,
                        "brand": brand,
                        "colour": colour,
                        "passengers": passengers,
                        "ts_start": ts_start.isoformat(),
                        "ts": ts_ms,
                    }

                    msg = self.serialize(key=brand, value=value)
                    self.produce(key=msg.key, value=msg.value)

            if not self.running:
                return

            print(f"Produced minute {minute_offset + 1}/60 ({ts.isoformat()})")

        print("Finished producing 12,000 vehicle messages.")


def main():
    app = Application(consumer_group="traffic_generator", auto_create_topics=True)
    source = VehicleTrafficGenerator(name="vehicle-traffic-producer")
    output_topic = app.topic(name=os.environ["output"])

    app.add_source(source=source, topic=output_topic)
    app.run()


if __name__ == "__main__":
    main()
