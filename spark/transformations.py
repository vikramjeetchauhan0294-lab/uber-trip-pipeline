from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def filter_high_fare(df: DataFrame) -> DataFrame:
    """
    Filter trips whose fare is greater than ₹300.
    """

    return df.filter(
        col("fare") > 300
    )