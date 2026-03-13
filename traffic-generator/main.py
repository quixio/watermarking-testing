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
    Generates 60 seconds of vehicle traffic data.
    Each discrete second has exactly 10 vehicles per colour (200 vehicles/sec).
    Total: 20 colours × 10 vehicles × 60 seconds = 12,000 messages.
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
        ts_start = datetime.now(timezone.utc)
        base_second = ts_start.replace(microsecond=0)

        for second_offset in range(60):
            second_start = base_second + timedelta(seconds=second_offset)
            second_start_ms = int(second_start.timestamp() * 1000)
            second_sent = 0

            for colour in COLOURS:
                for _ in range(10):
                    brand = random.choice(BRANDS)
                    plate = self._generate_plate()
                    passengers = random.randint(1, 4)
                    ts_ms = second_start_ms + random.randint(0, 999)

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
                    second_sent += 1
                    total_sent += 1

            if not self.running:
                print(f"Stopped early. Total messages sent: {total_sent}")
                return

            print(f"Produced second {second_offset + 1}/60 ({second_start.isoformat()}) — messages this second: {second_sent}, total sent: {total_sent}")

        print(f"Finished producing vehicle messages. Total sent: {total_sent}")


def main():
    app = Application(consumer_group="traffic_generator", auto_create_topics=True)
    source = VehicleTrafficGenerator(name="vehicle-traffic-producer")
    output_topic = app.topic(name=os.environ["output"])

    app.add_source(source=source, topic=output_topic)
    app.run()


if __name__ == "__main__":
    main()
