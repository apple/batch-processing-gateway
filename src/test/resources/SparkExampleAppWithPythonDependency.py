# This is a Spark application with two stages to trigger shuffle. People could specify the number of map
# tasks and reduce tasks for the shuffle.
# Example arguments: 4 2 (first argument is number of map tasks, second argument is number of reduce tasks)

import os, sys, getopt
from time import sleep
from pyspark.sql import SparkSession
from pyspark import TaskContext
import pythonLibExample
import collections

def list_env():
    print("Listing environment variables")
    for k, v in collections.OrderedDict(sorted(os.environ.items())).items():
        print(f'{k}={v}')

def list_files():
    file_dir = os.path.dirname(os.path.realpath(__file__))
    print(f"Listing files in application file directory: {file_dir}")
    if os.path.exists(file_dir):
        for f in os.listdir(file_dir):
            print(f)
    else:
        print(f"Directory not exist: {file_dir}")
    current_dir = os.getcwd()
    print(f"Listing files in current directory: {current_dir}")
    for f in os.listdir(current_dir):
        print(f)

if __name__ == "__main__":
    mapTasks = 4
    reduceTasks = 2
    sleepSeconds = 1

    print(f"Application arguments: {sys.argv}")

    if len(sys.argv) >= 2:
        mapTasks = int(sys.argv[1])

    if len(sys.argv) >= 3:
        reduceTasks = int(sys.argv[2])

    if len(sys.argv) >= 4:
        sleepSeconds = int(sys.argv[3])

    print(f"mapTasks: {mapTasks}, reduceTasks: {reduceTasks}")

    list_env()
    list_files()

    result = pythonLibExample.add_func(mapTasks, reduceTasks)
    print(f"Result: {mapTasks} + {reduceTasks} = {result}")

    spark = SparkSession \
        .builder \
        .appName("pyspark-app") \
        .getOrCreate()

    sparkContext=spark.sparkContext

    rdd = sparkContext.parallelize(range(0, mapTasks, 1), mapTasks)

    def f(iterator):
        taskContext = TaskContext.get()
        partitionId = taskContext.partitionId()
        list_env()
        list_files()
        print(f"Values for partition: {partitionId}")
        for x in iterator:
            print(f"{x}")
            result = pythonLibExample.add_func(x, x)
            print(f"Result: {x} + {x} = {result}")
            print(f"Sleeping {sleepSeconds} seconds in executor")
            sleep(sleepSeconds)

    rdd = rdd.repartition(reduceTasks)

    rdd.foreachPartition(f)

    print(f"Application finished")

    spark.stop()
