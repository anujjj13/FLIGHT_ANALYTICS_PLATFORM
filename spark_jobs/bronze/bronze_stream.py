from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# Create Spark session
spark = (
    SparkSession.builder
    .appName("BronzeLayer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9095")
    .option("subscribe", "flight_raw")
    .option("startingOffsets", "latest")
    .load()
)

# Bronze schema (no transformation)
bronze_df = kafka_df.select(
    current_timestamp().alias("ingestion_timestamp"),
    col("offset").alias("kafka_offset"),
    col("partition").alias("kafka_partition"),
    col("topic").alias("kafka_topic"),
    col("value").cast("string").alias("raw_json")
)

query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "data/checkpoints/bronze")
    .start("data/delta/bronze/flights")
)

query.awaitTermination()