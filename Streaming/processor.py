import findspark
findspark.init()

from pyspark.sql.functions import from_json, col
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    CHECKPOINT_DIR,
    PROCESSING_TIME,
    CSV_OUTPUT_PATH
)
from schema import message_schema
from spark_session import create_spark_session
import sys
import traceback


class DebeziumKafkaProcessor:
    def __init__(self):
        try:
            self.spark = create_spark_session("KafkaDebeziumMicroBatch")
        except Exception as e:
            print("Failed to create Spark session.")
            traceback.print_exc()
            sys.exit(1)
        self.df = None
        self.query = None

    def read_stream(self):
        try:
            self.df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", KAFKA_TOPIC) \
                .option("startingOffsets", "latest") \
                .load()
        except Exception as e:
            print("Error reading stream from Kafka:")
            traceback.print_exc()
            raise

    def transform_stream(self):
        try:
            value_df = self.df.selectExpr("CAST(value AS STRING) AS json_str")
            parsed_df = value_df.select(from_json(col("json_str"), message_schema).alias("data"))
            return parsed_df.select("data.after.*")
        except Exception as e:
            print("Error transforming stream:")
            traceback.print_exc()
            raise

    def write_to_csv(self, final_df):
        try:
            self.query = final_df.writeStream \
                .outputMode("append") \
                .format("csv") \
                .option("path", CSV_OUTPUT_PATH) \
                .option("checkpointLocation", CHECKPOINT_DIR) \
                .option("header", "true") \
                .trigger(processingTime=PROCESSING_TIME) \
                .start()
        except Exception as e:
            print("Error starting write stream to CSV:")
            traceback.print_exc()
            raise

    def run(self):
        try:
            self.read_stream()
            transformed_df = self.transform_stream()
            self.write_to_csv(transformed_df)
            self.query.awaitTermination()
        except KeyboardInterrupt:
            print("Streaming interrupted by user.")
        except Exception as e:
            print("Error during processing:")
            traceback.print_exc()
            sys.exit(1)
