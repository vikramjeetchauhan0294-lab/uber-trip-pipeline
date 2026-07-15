import json
import time

from kafka import KafkaProducer

from producer.trip_generator import generate_trip
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from logger.logger import logger


def create_producer():
    """
    Create and return a Kafka Producer.
    """

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )

    logger.info("Kafka Producer Connected Successfully")

    return producer


def send_trip(producer, trip):
    """
    Send a trip event to Kafka.
    """

    try:
        producer.send(
            topic=KAFKA_TOPIC,
            value=trip
        )

        producer.flush()

        logger.info(f"Sent {trip['trip_id']} to Kafka")

    except Exception as e:
        logger.error(f"Failed to send trip: {e}")


def main():
    """
    Main function to continuously generate and send trips.
    """

    producer = create_producer()

    trip_number = 1

    while True:

        trip = generate_trip(trip_number)

        send_trip(producer, trip)

        trip_number += 1

        time.sleep(1)


if __name__ == "__main__":
    main()