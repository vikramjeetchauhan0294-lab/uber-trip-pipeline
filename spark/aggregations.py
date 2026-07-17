from pyspark.sql import DataFrame
from pyspark.sql.functions import count


def trips_by_pickup_location(df: DataFrame) -> DataFrame:
    """
    Count total trips for each pickup location.
    """

    return (
        df
        .groupBy("pickup_location")
        .agg(
            count("*").alias("total_trips")
        )
    )