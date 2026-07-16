import json

from kafka import KafkaConsumer

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC
)

from logger.logger import logger


def create_consumer(group_id):
    """
    Create and return a Kafka Consumer.
    """

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8"))
    )

    logger.info(f"Kafka Consumer Connected Successfully")
    logger.info(f"Consumer Group: {group_id}")

    return consumer


def consume_messages(consumer):
    """
    Continuously consume messages from Kafka.
    """

    try:
        for message in consumer:

            trip = message.value

            logger.info(
                f"Partition: {message.partition} | "
                f"Offset: {message.offset} | "
                f"Trip ID: {trip['trip_id']} | "
                f"Driver: {trip['driver_id']} | "
                f"Fare: ₹{trip['fare']}"
            )

    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")

    finally:
        consumer.close()
        logger.info("Kafka Consumer Closed.")


def main():
    """
    Main entry point.
    """

    group_id = input("Enter Consumer Group Name: ")

    consumer = create_consumer(group_id)

    consume_messages(consumer)


if __name__ == "__main__":
    main()