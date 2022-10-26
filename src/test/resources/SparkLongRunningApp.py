# This is a Spark application that simply sleeps for the length of the specified time
# This can be used to test out features related to long-running jobs
# Example arguments: 20 (unit: minutes)

import sys
from time import sleep
from pyspark.sql import SparkSession


if __name__ == "__main__":
    print(f"Application arguments: {sys.argv}")

    if len(sys.argv) != 2:
        print("please specify sleep time in mins as arg!")

    sleep_seconds = int(sys.argv[1]) * 60

    print(f"sleep time: {sleep_seconds} seconds")

    spark = SparkSession \
        .builder \
        .appName("pyspark-long-running-app") \
        .getOrCreate()

    sparkContext = spark.sparkContext

    print(f"Night night!")
    sleep(sleep_seconds)
    print(f"Application finished")

    spark.stop()
