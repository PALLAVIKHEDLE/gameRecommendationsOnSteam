# from pyspark.sql import SparkSession
# import logging
# from config import KAFKA_TOPIC, BOOTSTRAP_SERVERS

# # Set up logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # Log the Kafka bootstrap servers configuration
# logger.info(f"Kafka bootstrap servers: {BOOTSTRAP_SERVERS}")

# # Create SparkSession with custom configurations
# spark = SparkSession.builder \
#     .appName("KafkaSparkStreamingApp") \
#     .config("spark.kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
#     .getOrCreate()

# # Check if SparkSession is created
# if spark:
#     logger.info("SparkSession created successfully")
# else:
#     logger.error("Failed to create SparkSession")

# # Define input DataFrame using Kafka source
# df = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
#     .option("subscribe", KAFKA_TOPIC) \
#     .load()

# # Log Kafka topic
# logger.info(f"Subscribed to Kafka topic: {KAFKA_TOPIC}")

# # Perform processing and display on console
# query = df \
#     .selectExpr("CAST(value AS STRING) AS message") \
#     .writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()

# # Await termination
# query.awaitTermination()

from pyspark.sql import SparkSession
import logging
from config import KAFKA_TOPIC, BOOTSTRAP_SERVERS

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Log the Kafka bootstrap servers configuration
logger.info(f"Kafka bootstrap servers: {BOOTSTRAP_SERVERS}")

# Create SparkSession with custom configurations
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingApp") \
    .config("spark.kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .getOrCreate()

# Check if SparkSession is created
if spark:
    logger.info("SparkSession created successfully")
else:
    logger.error("Failed to create SparkSession")

# Define input DataFrame using Kafka source
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Log Kafka topic
logger.info(f"Subscribed to Kafka topic: {KAFKA_TOPIC}")

# Perform processing and display on console
query = df \
    .selectExpr("CAST(value AS STRING) AS message") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

# Await termination
query.awaitTermination()
