import json

from kafka import KafkaConsumer

from config.settings import KAFKA_BOOTSTRAP_SERVERS
from logger.logger import logger


def create_consumer():

    consumer = KafkaConsumer(
        "uber-trips",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8"))
    )

    logger.info("Kafka Consumer Connected Successfully")

    return consumer


def main():

    consumer = create_consumer()

    for message in consumer:

        trip = message.value

        logger.info(trip)


if __name__ == "__main__":
    main()