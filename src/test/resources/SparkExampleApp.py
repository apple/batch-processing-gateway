# This is a Spark application with two stages to trigger shuffle. People could specify the number of map
# tasks and reduce tasks for the shuffle.
# Example arguments: 4 2 (first argument is number of map tasks, second argument is number of reduce tasks)

import sys, getopt
from time import sleep
from pyspark.sql import SparkSession
from pyspark import TaskContext
import os
import socket

if __name__ == "__main__":
    mapTasks = 2
    reduceTasks = 1
    sleepSeconds = 0

    print(f"Application arguments: {sys.argv}")

    if len(sys.argv) >= 2:
        mapTasks = int(sys.argv[1])

    if len(sys.argv) >= 3:
        reduceTasks = int(sys.argv[2])

    if len(sys.argv) >= 4:
        sleepSeconds = int(sys.argv[3])

    print(f"mapTasks: {mapTasks}, reduceTasks: {reduceTasks}")

    print(f"STATSD_SERVER_IP: {os.getenv('STATSD_SERVER_IP')}")
    print(f"STATSD_SERVER_PORT: {os.getenv('STATSD_SERVER_PORT')}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(bytes("siri.skate.statsd.test.driver:1|c|#tag1:value1,tag2:value2", "utf-8"), (os.getenv('STATSD_SERVER_IP'), int(os.getenv('STATSD_SERVER_PORT'))))
    print("Sent test metric to statsd server from driver")

    spark = SparkSession \
        .builder \
        .appName("pyspark-app") \
        .getOrCreate()

    sparkContext=spark.sparkContext

    rdd = sparkContext.parallelize(range(0, mapTasks, 1), mapTasks)

    def f(iterator):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(bytes("siri.skate.statsd.test.executor:1|c|#tag1:value1,tag2:value2", "utf-8"), (os.getenv('STATSD_SERVER_IP'), int(os.getenv('STATSD_SERVER_PORT'))))
        taskContext = TaskContext.get()
        partitionId = taskContext.partitionId()
        print(f"Values for partition: {partitionId}")
        for x in iterator:
            print(f"{x}")
            print(f"Sleeping {sleepSeconds} seconds in executor")
            sleep(sleepSeconds)

    rdd = rdd.repartition(reduceTasks)

    rdd.foreachPartition(f)

    rdd = rdd.map(lambda x: (f"STATSD_SERVER_IP in executor: {os.getenv('STATSD_SERVER_IP')}", f"STATSD_SERVER_PORT in executor: {os.getenv('STATSD_SERVER_PORT')}"))
    for element in rdd.collect():
        print(element)

    print(f"Application finished")

    spark.stop()
