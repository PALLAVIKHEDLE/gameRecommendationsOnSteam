import boto3
from kafka import KafkaProducer
import json
import logging
from config import KAFKA_TOPIC, BOOTSTRAP_SERVERS

# AWS S3 configuration
bucket_name = 'game-recommender-steam-tej'
object_keys = ['games_metadata.json', 'games.csv']

# Kafka configuration
kafka_servers = BOOTSTRAP_SERVERS
kafka_topic = KAFKA_TOPIC

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_servers, value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Initialize AWS S3 client
s3 = boto3.client('s3')

# Function to read data from S3 and produce it to Kafka topic
def send_to_kafka(bucket, keys):
    try:
        for key in keys:
            obj = s3.get_object(Bucket=bucket, Key=key)
            data = obj['Body'].read().decode('utf-8')
            
            if key.endswith('.json'):
                # Process JSON data line by line
                lines = data.split('\n')
                for line in lines:
                    if line.strip():  # Check if line is not empty
                        record = json.loads(line)
                        producer.send(kafka_topic, value=record)
            elif key.endswith('.csv'):
                # Process CSV data
                lines = data.split('\n')
                header = lines[0].split(',')
                for line in lines[1:]:
                    if line.strip():  # Check if line is not empty
                        values = line.split(',')
                        record = dict(zip(header, values))
                        producer.send(kafka_topic, value=record)
        
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
