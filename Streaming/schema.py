# schema.py

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

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
