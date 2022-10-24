#-- test.py --

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("show databases").show(truncate=False)
spark.sql("use default").show(truncate=False)
spark.sql("show tables").show(truncate=False)
