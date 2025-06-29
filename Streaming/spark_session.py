import findspark
findspark.init()

import os
os.environ["HADOOP_HOME"] = "C:/hadoop"

from pyspark.sql import SparkSession

def create_spark_session(app_name: str) -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark
