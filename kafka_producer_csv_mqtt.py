
from kafka import KafkaProducer
import csv
import time

KAFKA_TOPIC_NAME = "test-topic"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")
    # Create a Kafka producer
    kafka_producer_obj = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS])

    # Read data from CSV file
    with open('/home/gharsallah/flotte/filestream/input_data/csv/2017-fordgobike-tripdata.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Send each row as a message to the Kafka topic
            kafka_producer_obj.send(KAFKA_TOPIC_NAME, str.encode(str(row)))



    # Close the Kafka producer
    kafka_producer_obj.close()


