# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
from quixstreams import Application
from quixstreams.dataframe import StreamingDataFrame

import os

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()


def flatten_colours(row: dict) -> dict:
    """Flatten the windowed colour counts into a single row using 'end' as timestamp."""
    flat = {"time": row["end"]}
    flat.update(row["value"])
    return flat


def define_pipeline(sdf: StreamingDataFrame):

    sdf = sdf.apply(flatten_colours)

    # Set row timestamp from the 'end' field (milliseconds → nanoseconds for Quix)
    sdf = sdf.set_timestamp(lambda row, *_: row["time"])

    # Optional printing for debugging.
    #sdf = sdf.print(metadata=True)

    return sdf


def main():

    # Setup necessary objects
    app = Application(
        consumer_group="my_transformation",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    input_topic = app.topic(name="colours")
    output_topic = app.topic(name=os.environ["output"])
    sdf = app.dataframe(topic=input_topic)

    # Apply the pipeline transformations
    sdf = define_pipeline(sdf)

    # Finish off by writing to the final result to the output topic
    sdf.to_topic(output_topic)

    # With our pipeline defined, now run the Application
    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()