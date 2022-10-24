from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("pyspark-hive") \
        .getOrCreate()

    spark.sql("show databases").show()

    spark.sql("create database if not exists test").show()

    spark.sql("create table if not exists test.test_table (id INT, str STRING)").show()

    spark.sql("insert overwrite table test.test_table values (1, 'str1'), (2, 'str2')").show()

    spark.sql("select * from test.test_table union all select 100 id, 'str100' str").show()

    spark.stop()