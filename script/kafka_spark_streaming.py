from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from config import KAFKA_TOPIC, BOOTSTRAP_SERVERS

# Define the Kafka topic and bootstrap servers
KAFKA_TOPIC = KAFKA_TOPIC
BOOTSTRAP_SERVERS = BOOTSTRAP_SERVERS

# Define the schema for JSON parsing
json_schema = StructType([
    StructField("app_id", StringType(), nullable=True),
    StructField("title", StringType(), nullable=True),
    StructField("date_release", StringType(), nullable=True),
    StructField("win", StringType(), nullable=True),
    StructField("mac", StringType(), nullable=True),
    StructField("linux", StringType(), nullable=True),
    StructField("rating", StringType(), nullable=True),
    StructField("positive_ratio", StringType(), nullable=True),
    StructField("user_reviews", StringType(), nullable=True),
    StructField("price_final", StringType(), nullable=True),
    StructField("price_original", StringType(), nullable=True),
    StructField("discount", StringType(), nullable=True),
    StructField("steam_deck", StringType(), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("tags", ArrayType(StringType()), nullable=True)
])

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingApp") \
    .getOrCreate()

# Define input DataFrame using Kafka source with specific partitions
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# Convert the value column from bytes to string
df = df.withColumn("value", df["value"].cast("string"))

# Parse JSON data based on the schema
parsed_df = df.withColumn("jsonData", from_json(col("value"), json_schema)) \
    .select("jsonData.*") \
    .filter("jsonData is not null")

# Filter data from specific partitions (0 and 1)
filtered_df = parsed_df.filter((col("partition") == 0) | (col("partition") == 1))

# Print the schema of the DataFrame
print("Schema of the DataFrame:")
filtered_df.printSchema()

# Display the parsed data continuously in the console
query = filtered_df \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("numRows", 125) \
    .start()

# Wait for the stream to finish
query.awaitTermination()
