from confluent_kafka import Consumer, KafkaException, TopicPartition
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka configuration
kafka_config = {
    'bootstrap.servers': '3.145.153.231:9092',  # Replace with your Kafka broker(s)
    'group.id': 'streaming_data_processor',       # Consumer group ID
    'auto.offset.reset': 'earliest'               # Start reading from the beginning of the topic if no offset is stored
}

# Kafka topic to consume from
kafka_topic = 'demoWithoutPartition'

def process_message(msg_value):
    # Process the message (e.g., transform it, store it, etc.)
    logging.info(f"Received message: {msg_value}")

def main():
    # Create a Kafka consumer
    consumer = Consumer(kafka_config)
    logging.info("Consumer created")

    # Assign the consumer to partition 0 of the Kafka topic
    partitions = [TopicPartition(kafka_topic, 0)]
    consumer.assign(partitions)
    logging.info("Consumer assigned to partitions")

    try:
        while True:
            # Poll for messages
            msg = consumer.poll(timeout=1.0)  # Adjust the timeout as needed
    
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
                    break
    
            # Decode the message value and process the message
            try:
                message_value = json.loads(msg.value().decode('utf-8'))
                logging.info(f"Received message: {message_value}")
                # process_message(message_value)
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
