from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from config import KAFKA_TOPIC, BOOTSTRAP_SERVERS

# Define the Kafka topic and bootstrap servers
KAFKA_TOPIC = KAFKA_TOPIC
BOOTSTRAP_SERVERS = BOOTSTRAP_SERVERS

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingApp") \
    .getOrCreate()

# Define input DataFrame using Kafka source with "earliest" starting offset
df_earliest = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert the value column from bytes to string
df_earliest = df_earliest.withColumn("value", df_earliest["value"].cast("string"))

# Define a flexible schema for JSON parsing
schema = "app_id STRING, title STRING, date_release STRING, win STRING, " \
         "mac STRING, linux STRING, rating STRING, positive_ratio STRING, " \
         "user_reviews STRING, price_final FLOAT, price_original FLOAT, " \
         "discount FLOAT, steam_deck BOOLEAN, description STRING, tags ARRAY<STRING>"

# Parse JSON data into columns using the defined schema
parsed_df_earliest = df_earliest.withColumn("jsonData", from_json(col("value"), schema)) \
    .select("jsonData.*")

# Print the schema of the DataFrame
print("Schema of the DataFrame (Earliest):")
parsed_df_earliest.printSchema()

# Display the parsed JSON data continuously in the console
print("Parsed JSON Data (Earliest):")
query_earliest = parsed_df_earliest \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

# Don't call awaitTermination() to keep the streaming query running indefinitely
query_earliest.awaitTermination()
