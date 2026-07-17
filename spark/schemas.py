from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

trip_schema = StructType([

    StructField("trip_id", StringType(), True),

    StructField("driver_id", StringType(), True),

    StructField("rider_id", StringType(), True),

    StructField("pickup_location", StringType(), True),

    StructField("dropoff_location", StringType(), True),

    StructField("distance_km", DoubleType(), True),

    StructField("fare", DoubleType(), True),

    StructField("trip_status", StringType(), True),

    StructField("event_timestamp", StringType(), True)

])