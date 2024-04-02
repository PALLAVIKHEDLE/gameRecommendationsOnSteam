from pyspark.sql import SparkSession
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create SparkSession with custom configurations
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingApp") \
    .config("spark.kafka.bootstrap.servers", "3.145.153.231:9092") \
    .config("spark.kafka.consumer.key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") \
    .config("spark.kafka.consumer.value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") \
    .config("spark.kafka.consumer.auto.offset.reset", "latest") \
    .config("spark.kafka.consumer.enable.auto.commit", "true") \
    .getOrCreate()

# Kafka configuration
kafka_servers = '3.145.153.231:9092'  # Kafka broker address
# kafka_topic = 'streamAnalysis'
kafka_topic='demoWithoutPartition'

# Define input DataFrame using Kafka source
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Perform processing and print message content
query = df \
    .selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .foreach(lambda row: logger.info(row.asDict())) \
    .outputMode("append") \
    .start()

# Wait for termination
query.awaitTermination()
