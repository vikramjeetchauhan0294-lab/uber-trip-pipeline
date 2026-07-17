from pyspark.sql import SparkSession


def main():

    spark = (
        SparkSession.builder
        .appName("Read CSV Example")
        .master("local[*]")
        .getOrCreate()
    )

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/trips.csv")
    )

    print("\n===== DATA =====")
    df.show()

    print("\n===== SCHEMA =====")
    df.printSchema()

    print("\n===== TOTAL RECORDS =====")
    print(df.count())

    spark.stop()


if __name__ == "__main__":
    main()