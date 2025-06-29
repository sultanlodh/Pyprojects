import findspark
findspark.init()

import os
os.environ["HADOOP_HOME"] = "C:/hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class KafkaDebeziumProcessor:
    def __init__(self, kafka_bootstrap_servers, topic, checkpoint_location, minio_config):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.topic = topic
        self.checkpoint_location = checkpoint_location
        self.minio_config = minio_config
        self.spark = self._create_spark_session()
        self.message_schema = self._define_schema()

    def _create_spark_session(self):
        spark = SparkSession.builder \
            .appName("KafkaDebeziumToHudi") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.hadoop.fs.s3a.endpoint", self.minio_config["endpoint"]) \
            .config("spark.hadoop.fs.s3a.access.key", self.minio_config["access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", self.minio_config["secret_key"]) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.jars.packages",
                    ",".join([
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                        "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.14.1",
                        "org.apache.hadoop:hadoop-aws:3.3.2"
                    ])
                   ) \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        return spark

    def _define_schema(self):
        after_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("tracking_number", StringType(), True),
            StructField("current_status", StringType(), True),
            StructField("createTs", StringType(), True),
            StructField("modifyTs", StringType(), True),
        ])

        message_schema = StructType([
            StructField("before", StringType(), True),
            StructField("after", after_schema, True),
            StructField("op", StringType(), True),
            StructField("ts_ms", StringType(), True),
        ])
        return message_schema

    def read_kafka_stream(self):
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "latest") \
            .load()

    def process_stream(self, df):
        value_df = df.selectExpr("CAST(value AS STRING) AS json_str")
        parsed_df = value_df.select(from_json(col("json_str"), self.message_schema).alias("data"))
        after_df = parsed_df.select("data.after.*")
        return after_df

    def write_to_hudi(self, after_df, table_name, base_path, trigger_interval="30 seconds"):
        hudi_options = {
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.recordkey.field': 'id',
            'hoodie.datasource.write.partitionpath.field': '',  # no partition
            'hoodie.datasource.write.table.name': table_name,
            'hoodie.datasource.write.operation': 'upsert',
            'hoodie.upsert.shuffle.parallelism': '2',
            'hoodie.insert.shuffle.parallelism': '2',
            'hoodie.datasource.write.hive_style_partitioning': 'false',
            'hoodie.datasource.write.precombine.field': 'createTs',
            'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
            'hoodie.datasource.write.keygenerator.class':
                'org.apache.hudi.keygen.NonpartitionedKeyGenerator'
        }

        query = after_df.writeStream \
            .format("hudi") \
            .options(**hudi_options) \
            .option("checkpointLocation", self.checkpoint_location) \
            .trigger(processingTime=trigger_interval) \
            .outputMode("append") \
            .start(base_path)

        query.awaitTermination()


if __name__ == "__main__":
    minio_config = {
        "endpoint": "http://localhost:9001",
        "access_key": "admin",       # replace with your actual access key
        "secret_key": "password",       # replace with your actual secret key
    }

    processor = KafkaDebeziumProcessor(
        kafka_bootstrap_servers="localhost:9092",
        topic="mysql_server.mydb.consignment",
        checkpoint_location="file:///C:/spark_checkpoints/consignment/",
        minio_config=minio_config
    )

    kafka_df = processor.read_kafka_stream()
    processed_df = processor.process_stream(kafka_df)

    # Replace `your-bucket/your-folder` with actual MinIO bucket path
    processor.write_to_hudi(
        after_df=processed_df,
        table_name="consignment_hudi_table",
        base_path="s3a://databuket/"
    )
