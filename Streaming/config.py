# Configuration file for Spark Structured Streaming with Kafka and MySQL
# This file contains the necessary configurations for connecting to Kafka, reading from a topic,
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "mysql_server.mydb.consignment"
CHECKPOINT_DIR = "file:///C:/spark_checkpoints/"

PROCESSING_TIME = "30 seconds"
CSV_OUTPUT_PATH = "file:///C:/spark_output/csv_data/"
#SPARK_JARS_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"    