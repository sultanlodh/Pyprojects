import findspark
findspark.init()

from pyspark.sql import SparkSession

# ✅ Output location in MinIO
output_path = "s3a://databuket/test_upload_text_file/"

# ✅ Create SparkSession
spark = SparkSession.builder \
    .appName("SparkWriteToMinIO") \
    .config("spark.jars.packages", ",".join([
        "org.apache.hadoop:hadoop-aws:3.3.2",
        "com.amazonaws:aws-java-sdk-bundle:1.11.1026"
    ])) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ✅ Simple test RDD
rdd = spark.sparkContext.parallelize(["Line 1 from Spark", "Line 2 written to MinIO"])

# ✅ Save to MinIO
rdd.saveAsTextFile(output_path)

print(f"\n✅ File written successfully to: {output_path}")

spark.stop()
