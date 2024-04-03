import logging
from kafka import KafkaConsumer
from config import KAFKA_TOPIC, BOOTSTRAP_SERVERS

# Kafka configuration
kafka_servers = BOOTSTRAP_SERVERS # Kafka broker address
kafka_topic = KAFKA_TOPIC

# Initialize Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_servers,
    auto_offset_reset='earliest',  # Start consuming from the beginning of the topic
    enable_auto_commit=True,  # Automatically commit offsets
    value_deserializer=lambda x: x.decode('utf-8')  # Deserialize message value as UTF-8 string
)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Log topic and consumer group
logging.info(f"Subscribed to topic: {kafka_topic}")

# Function to consume messages from Kafka topic
def consume_messages():
    try:
        for message in consumer:
            logging.info(f"Received message: {message.value}")
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        consumer.close()

# Execute function
if __name__ == "__main__":
    consume_messages()
