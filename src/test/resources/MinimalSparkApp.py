# A minimal Spark app for testing

from pyspark.sql import SparkSession


if __name__ == "__main__":
    print(f"Running a minimal Spark app that simply gets the Spark context.")

    spark = SparkSession \
        .builder \
        .appName("minimal-pyspark-app") \
        .getOrCreate()

    sparkContext = spark.sparkContext

    print(f"Application finished.")

    spark.stop()
