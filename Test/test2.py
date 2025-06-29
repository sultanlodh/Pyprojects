import findspark
findspark.init()

import os
os.environ["HADOOP_HOME"] = "C:/hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define schema for Debezium `after` payload
after_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("tracking_number", StringType(), True),
    StructField("current_status", StringType(), True),
    StructField("createTs", StringType(), True),
    StructField("modifyTs", StringType(), True),
])

# Define outer Debezium message schema
message_schema = StructType([
    StructField("before", StringType(), True),
    StructField("after", after_schema, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", StringType(), True),
])

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaDebeziumMicroBatch") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .getOrCreate()

# Reduce verbosity in logs
spark.sparkContext.setLogLevel("WARN")

# Read streaming data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "mysql_server.mydb.consignment") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka value from binary to string
value_df = df.selectExpr("CAST(value AS STRING) AS json_str")

# Parse JSON using schema
parsed_df = value_df.select(from_json(col("json_str"), message_schema).alias("data"))

# Extract 'after' field (actual change data)
after_df = parsed_df.select("data.after.*")

# Write stream to console with micro-batch trigger every 10 seconds
query = after_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "file:///C:/spark_checkpoints/") \
    .trigger(processingTime="30 seconds") \
    .start()

# Keep the stream alive
query.awaitTermination()
