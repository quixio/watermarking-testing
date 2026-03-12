# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
from quixstreams import Application
from quixstreams.dataframe.windows import Count

import os
import time
import random
from datetime import timedelta
# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()


def main():
    """
    Transformations generally read from, and produce to, Kafka topics.

    They are conducted with Applications and their accompanying StreamingDataFrames
    which define what transformations to perform on incoming data.

    Be sure to explicitly produce output to any desired topic(s); it does not happen
    automatically!

    To learn about what operations are possible, the best place to start is:
    https://quix.io/docs/quix-streams/processing.html
    """

    # Setup necessary objects
    app = Application(
        consumer_group=f"my_transformation_v7_{random.Random(int(time.time() * 1000)).randint(0, 999999)}",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )

    def custom_ts_extractor(value, headers, timestamp, timestamp_type):
        return value["ts"]  #


    input_topic = app.topic(name=os.environ["input"], timestamp_extractor=custom_ts_extractor)
    output_topic = app.topic(name=os.environ["output"])
    sdf = app.dataframe(topic=input_topic)

    # Repartition by colour so all messages with the same colour
    # end up on the same partition
    sdf = sdf.group_by("colour")

   

    colour_counts = {}

    
    def count_colour(row):
        colour = row["colour"]
        colour_counts[colour] = colour_counts.get(colour, 0) + 1
        row["colour_count"] = colour_counts[colour]
        return row

    sdf = sdf.apply(count_colour)

    sdf.print_table(
        size=20,
        title="Colours",
        columns=["colour", "colour_count"]
    )

    sdf = (
        sdf
        # Define a hopping window of 1h with 10m step
        # You can also pass duration_ms and step_ms as integers of milliseconds
        .tumbling_window(duration_ms=timedelta(minutes=1))
        
        # Specify the "mean" aggregate function
        .agg(colour_count=Count())

        # Emit updates for each incoming message
        .final()
    )
    sdf.print()


    #sdf.to_topic(output_topic)

    # With our pipeline defined, now run the Application
    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()
