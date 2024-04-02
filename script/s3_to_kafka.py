# import boto3
# from kafka import KafkaProducer

# # AWS S3 configuration
# bucket_name = 'game-recommender-steam-tej'
# # object_keys = ['games_metadata.json', 'games.csv', 'recommendations.csv', 'users.csv']
# object_keys = ['games_metadata.json', 'games.csv']

# # Kafka configuration
# kafka_servers = ['3.145.153.231:9092']  # Kafka broker address
# kafka_topic = 'streamAnalysis'

# # Initialize Kafka producer
# producer = KafkaProducer(bootstrap_servers=kafka_servers)

# # Initialize AWS session with specified profile
# session = boto3.Session(profile_name='tejashwa-admin')
# # Initialize S3 client using the session
# s3 = session.client('s3')

# # Function to read data from S3 and produce it to Kafka topic
# def send_to_kafka(bucket, keys):
#     try:
#         for key in keys:
#             obj = s3.get_object(Bucket=bucket, Key=key)
#             # Serialize data if necessary (e.g., JSON)
#             # Send the bytes directly to Kafka
#             for line in obj['Body'].iter_lines():
#                 if line:
#                     print("Sending message:", line)  # Print the message before sending
#                     producer.send(kafka_topic, value=line)
#         producer.flush()  # Ensure all buffered messages are sent
#         print("All messages sent successfully to Kafka")
#     except Exception as e:
#         print(f"An error occurred: {str(e)}")
#     finally:
#         producer.close()  # Close the Kafka producer

# # Execute function
# send_to_kafka(bucket_name, object_keys)

import boto3
from kafka import KafkaProducer
import json
import logging


# AWS S3 configuration
bucket_name = 'game-recommender-steam-tej'
object_keys = ['games_metadata.json', 'games.csv']

# Kafka configuration
kafka_servers = ['3.145.153.231:9092']  # Kafka broker address
kafka_topic = 'demoWithoutPartition'


# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_servers)

# Initialize AWS S3 client
s3 = boto3.client('s3')

# Function to read data from S3 and produce it to Kafka topic
def send_to_kafka(bucket, keys):
    try:
        for key in keys:
            obj = s3.get_object(Bucket=bucket, Key=key)
            data = obj['Body'].read().decode('utf-8')
            messages = data.split('\n')
            for message in messages:
                if message:
                    producer.send(kafka_topic, value=message.encode('utf-8'))
        producer.flush()
        logging.info("All messages sent successfully to Kafka")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        producer.close()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Execute function
send_to_kafka(bucket_name, object_keys)
