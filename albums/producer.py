from confluent_kafka import Producer, Consumer, KafkaException
import pandas as pd
import json

def read_config():
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config

def produce_from_csv(topic, config, csv_file):
    producer = Producer(config)
    
    # Read data from CSV file
    albums = pd.read_csv(csv_file)

    # Send each record to the Kafka topic
    for record in albums.to_dict(orient='records'):
        data = json.dumps(record).encode('utf-8')  # Convert each row to JSON
        
        # Produce the message to the Kafka topic
        try:
            producer.produce(topic, value=data)
            producer.poll(0)  # Poll to handle delivery reports and callbacks
            print(f"Produced message to topic {topic}: {record}")
        except Exception as e:
            print(f"Error producing message: {e}")
    
    # Flush to ensure all messages are sent
    producer.flush()
    print("All messages from CSV have been produced.")

# Main code for the producer
if __name__ == "__main__":
    config = read_config()
    topic = "albums"  # Change to your desired topic
    csv_file = 'albums/albums.csv'
    produce_from_csv(topic, config, csv_file)