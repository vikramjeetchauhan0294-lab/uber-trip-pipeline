from pyspark.sql import SparkSession


def create_spark_session():
    """
    Create and return a Spark Session.
    """

    spark = (
        SparkSession.builder
        .appName("Uber Trip Pipeline")
        .master("local[*]")
        .getOrCreate()
    )

    return spark


def main():

    spark = create_spark_session()

    print("=" * 50)
    print("Spark Session Created Successfully!")
    print("=" * 50)

    print(f"Spark Version : {spark.version}")
    print(f"Application Name : {spark.sparkContext.appName}")
    print(f"Master : {spark.sparkContext.master}")

    spark.stop()

    print("\nSpark Session Closed Successfully.")


if __name__ == "__main__":
    main()