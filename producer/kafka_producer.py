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

    logger.info("✅ Kafka Producer Connected Successfully")

    return producer


def send_trip(producer, trip):
    """
    Send a trip event to Kafka and log partition & offset.
    """

    try:
        # Send the message to Kafka
        future = producer.send(
            topic=KAFKA_TOPIC,
            value=trip
        )

        # Wait for Kafka acknowledgement
        metadata = future.get(timeout=10)

        # Flush pending messages
        producer.flush()

        # Log success
        logger.info(
            f"Trip {trip['trip_id']} stored in "
            f"Partition {metadata.partition}, "
            f"Offset {metadata.offset}"
        )

    except Exception as e:
        logger.error(f"❌ Failed to send trip: {e}")


def main():
    """
    Continuously generate Uber trips and send them to Kafka.
    """

    producer = create_producer()

    trip_number = 1

    try:
        while True:

            trip = generate_trip(trip_number)

            send_trip(producer, trip)

            trip_number += 1

            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("🛑 Producer stopped by user.")

    finally:
        producer.close()
        logger.info("Kafka Producer Closed.")


if __name__ == "__main__":
    main()