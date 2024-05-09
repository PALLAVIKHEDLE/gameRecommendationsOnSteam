import boto3
from kafka import KafkaProducer
import json
import logging
from config import KAFKA_TOPIC, BOOTSTRAP_SERVERS

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, 
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))
# Initialize AWS S3 client
s3 = boto3.client('s3')

def send_to_kafka(bucket, keys, kafka_topic):
    try:
        for key in keys:
            obj = s3.get_object(Bucket=bucket, Key=key)
            data = obj['Body'].read().decode('utf-8')
            
            if key.endswith('.json'):
                # Process JSON data
                records = [json.loads(line) for line in data.split('\n') if line.strip()]
                for record in records:
                    # Send JSON record to partition 0
                    producer.send(kafka_topic, value=record, partition=0)
            elif key.endswith('.csv'):
                # Process CSV data
                lines = data.split('\n')
                header = lines[0].split(',')
                for line in lines[1:]:
                    if line.strip():  # Check if line is not empty
                        values = line.split(',')
                        record = dict(zip(header, values))
                        # Send CSV record to partition 1
                        producer.send(kafka_topic, value=record, partition=1)
        
        # Flush producer to ensure all messages are sent
        producer.flush()
        logging.info("All messages sent successfully to Kafka")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        producer.close()

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# AWS S3 configuration
bucket_name = 'game-recommender-steam-tej'
object_keys = ['games_metadata.json', 'games.csv', 'recommendations.csv', 'user.csv']

# Kafka configuration
kafka_topic = KAFKA_TOPIC

# Execute function
send_to_kafka(bucket_name, object_keys, kafka_topic)
