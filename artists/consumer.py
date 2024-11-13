from confluent_kafka import Consumer, KafkaException
import pandas as pd
import json
import random
import string
import boto3
from io import StringIO

# Reads the client configuration from the properties file
def read_config():
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config

# Generates a random string of specified length
def generate_random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length))

# Initializes the Kafka Consumer with Confluent configuration
def create_consumer(config, topic):
    config["group.id"] = "YOUR_CONSUMER_GROUP"
    config["auto.offset.reset"] = "earliest"
    consumer = Consumer(config)
    consumer.subscribe([topic])
    return consumer

# Main function to consume messages, aggregate, and upload to S3
def consume_and_upload_to_s3(consumer, bucket_name):
    s3_client = boto3.client('s3')
    data_buffer = []

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    continue

            # Parse the message and add it to the data buffer
            record = json.loads(msg.value().decode('utf-8'))
            data_buffer.append(record)

            # If buffer reaches the threshold, upload to S3
            if len(data_buffer) >= 10:
                df = pd.DataFrame(data_buffer)
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)
                
                # Generate a unique S3 key
                s3_key = f"staging/artists/{generate_random_string(20)}.csv"
                
                # Upload the CSV to S3
                s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
                print(f"DataFrame uploaded successfully to S3 bucket: {bucket_name} with key: {s3_key}")
                
                # Clear the buffer
                data_buffer = []
    except KeyboardInterrupt:
        print("Consumer interrupted by user.")
    finally:
        consumer.close()
        print("Consumer closed.")

# Main function to execute the consumer and upload logic
if __name__ == "__main__":
    config = read_config()
    topic = "artist"  # Your Kafka topic
    bucket_name = 'devandykafka'  # Your S3 bucket name

    consumer = create_consumer(config, topic)
    consume_and_upload_to_s3(consumer, bucket_name)
