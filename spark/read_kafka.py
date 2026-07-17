from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

from spark.schemas import trip_schema
from spark.transformations import filter_high_fare
from spark.aggregations import trips_by_pickup_location


def create_spark_session():
    """
    Create and return a Spark Session configured for Kafka Streaming.
    """

    spark = (
        SparkSession.builder
        .appName("Uber Trip Kafka Streaming")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6"
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def main():

    spark = create_spark_session()

    # Read streaming data from Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "uber-trips-partitioned")
        .option("startingOffsets", "earliest")
        .load()
    )

    # Convert Kafka binary value into JSON string
    json_df = kafka_df.selectExpr(
        "CAST(value AS STRING) AS json_string"
    )

    # Parse JSON string into Spark columns
    parsed_df = (
        json_df
        .select(
            from_json(
                col("json_string"),
                trip_schema
            ).alias("trip")
        )
        .select("trip.*")
    )
    high_fare_df = filter_high_fare(parsed_df)
    pickup_df = trips_by_pickup_location(high_fare_df)

    # Write streaming data to console
    query = (
        pickup_df
        .writeStream
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()