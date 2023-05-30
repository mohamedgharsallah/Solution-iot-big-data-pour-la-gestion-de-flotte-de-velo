import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
import csv

#mqtt
import paho.mqtt.client as mqtt
from pykafka import KafkaClient
import csv

broker_address = "test.mosquitto.org"
topic = "esp32"

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 pyspark-shell'

kafka_topic_name = "test-topic"
kafka_bootstrap_servers = 'localhost:9092'



kafka_client = KafkaClient(hosts='localhost:9092')
kafka_topic = kafka_client.topics[b'test-topic']
kafka_producer = kafka_topic.get_sync_producer()





if __name__ == "__main__":
    print("Welcome!!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("spark") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from test-topic
    orders_df = spark \
        .readStream \
        .format("kafka") \
\
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    # Define schema for incoming Kafka messages
    orders_schema = "duration_sec STRING, start_time STRING, end_time STRING, " \
                    "start_station_id LONG, start_station_name STRING, start_station_latitude DOUBLE, " \
                    "start_station_longitude DOUBLE, end_station_id LONG, end_station_name STRING, " \
                    "end_station_latitude DOUBLE, end_station_longitude DOUBLE, bike_id STRING, user_type STRING"

    # Parse incoming messages as JSON and apply the schema
    orders_df1 = orders_df.select(from_json(col("value").cast("string"), orders_schema).alias("data"), "timestamp")
    orders_df1 = orders_df1.selectExpr("data.*", "timestamp")

    # The most popular start and end stations
    popular_stations = orders_df1.groupBy("start_station_name", "end_station_name") \
        .count() \
        .orderBy(desc("count")) \
        .limit(10)

    # The most frequently rented bikes
    popular_bikes = orders_df1.groupBy("bike_id") \
        .count() \
        .orderBy(desc("count")) \
        .limit(10)

    # The average duration of a rental
    avg_duration = orders_df1.select(avg("duration_sec").cast("FLOAT").alias("avg_duration_sec"))

    # The busiest times for bike rentals
    #busiest_times = orders_df1.groupBy(window("start_time", "120 minutes")) \
     #   .count() \
      #  .orderBy(desc("count")) \
       # .limit(10)


    # Write results into console for debugging purpose
    popular_stations_write_stream = popular_stations \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("complete") \
        .option("truncate", "true") \
        .format("console") \
        .start()

    time.sleep(2)
    popular_bikes_write_stream = popular_bikes \
        .writeStream \
        .trigger(processingTime='7 seconds') \
        .outputMode("complete") \
        .option("truncate", "true") \
        .format("console") \
        .start()
    time.sleep(2)
    avg_duration_write_stream = avg_duration \
        .writeStream \
        .trigger(processingTime='9 seconds') \
        .outputMode("complete") \
        .option("truncate", "true") \
        .format("console") \
        .start()

#busiest_times_write_stream = busiest_times \
 #   .writeStream \
  #  .trigger(processingTime='10 seconds') \
   # .outputMode("complete") \
    #.option("truncate", "false") \
    #.format("console") \
    #.start()

#mqtt


    def on_connect(client, userdata, flags, rc):
        print("Connected to MQTT broker with result code " + str(rc))
        client.subscribe(topic)


    def on_message(client, userdata, msg):
        data = str(msg.payload.decode("utf-8"))
        print("Received data: " + data)
        with open('/home/gharsallah/Downloads/es.csv', mode='a') as file:
            writer = csv.writer(file)
            writer.writerow([data])


    client = mqtt.Client("mmmm")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker_address)

    client.loop_forever()
time.sleep(2)
spark.streams.awaitAnyTermination()  # Wait for any of the streams to terminate
