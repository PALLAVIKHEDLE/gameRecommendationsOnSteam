from confluent_kafka import Consumer, KafkaException, TopicPartition
import json
import logging
from config import BOOTSTRAP_SERVERS, KAFKA_TOPIC

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka configuration
kafka_config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,  # Replace with your Kafka broker(s)
    'group.id': "test-group-3",
    'auto.offset.reset': "earliest",  # Start reading from the beginning of the topic if no offset is stored
}

# Kafka topic to consume from
kafka_topic = KAFKA_TOPIC

def process_message(partition, msg_value):
    # Process the message (e.g., transform it, store it, etc.)
    logging.info(f"Received message from partition {partition}: {msg_value}")

def main():
    # Partition to consume from
    partition = 0

    # Create a Kafka consumer
    consumer = Consumer(kafka_config)
    logging.info("Consumer created")

    # Assign consumer to partition 0
    consumer.assign([TopicPartition(kafka_topic, partition)])
    logging.info(f"Assigned to partition: {partition, kafka_topic}")

    try:
        while True:
            # Poll for messages
            logging.debug("Polling for messages...")
            msg = consumer.poll(1.0)  # Adjust the timeout as needed
        
            if msg is None:
                logging.debug("No message received")
                continue

            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    # End of partition event, not an error
                    logging.debug("End of partition reached")
                    continue
                else:
                    logging.error(f"Kafka Error: {msg.error()}")
                    logging.error(f"Error code: {msg.error().code()}")  # Additional logging for error code
                    logging.error(f"Error reason: {msg.error().str()}")  # Additional logging for error reason
                    break

            # Decode the message value and process the message
            try:
                partition = msg.partition()
                message_value = json.loads(msg.value().decode('utf-8'))
                logging.info(f"Received message: {message_value}")
                process_message(partition, message_value)
            except json.JSONDecodeError as e:
                logging.error(f"JSON Decode Error: {e}")

            # Commit the message offset if message processed successfully
            consumer.commit(asynchronous=False)
            logging.debug("Offset committed")

    except KeyboardInterrupt:
        logging.info("Shutdown signal received.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
        logging.info("Consumer closed")

if __name__ == "__main__":
    main()
