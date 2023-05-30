import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
import csv
import psycopg2
#mqtt
import paho.mqtt.client as mqtt
from pykafka import KafkaClient

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 pyspark-shell'

kafka_topic_name = "test-topic"
kafka_bootstrap_servers = 'localhost:9092'
#mqtt
mqttBroker = "mqtt.eclipseprojects.io"
client = mqtt.Client("mqttbridge")
client.connect(mqttBroker)

kafka_client = KafkaClient(hosts='localhost:9092')
kafka_topic = kafka_client.topics[b'test-topic']
kafka_producer = kafka_topic.get_sync_producer()

if __name__ == "__main__":
    print("Welcome!!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("mm") \
        .config("spark.driver.extraClassPath", "/home/gharsallah/Downloads/postgresql-42.3.1.jar") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # configure the PostgreSQL JDBC driver
    jdbc_url = "jdbc:postgresql://localhost:5432/pfadata"
    connection_properties = {
        "user": "sara",
        "password": "saroura1",
        "driver": "org.postgresql.Driver"
    }
    # Connect to PostgreSQL and truncate the tables
    conn = psycopg2.connect(
        host="localhost",
        database="pfadata",
        user="sara",
        password="saroura1"
    )
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE popular_station, popular_bikes, avg_duration, mqtt_msg;")
    conn.commit()
    cur.close()


    # Construct a streaming DataFrame that reads from test-topic
    orders_df = spark \
        .readStream \
        .format("kafka") \
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

    # Write results to PostgreSQL table
    def write_to_postgres_pop_sation(df, epoch_id):
        df.write.jdbc(url=jdbc_url, table="popular_station", mode="append", properties=connection_properties)
    def write_to_postgres_popular_bikes(df, epoch_id):
        df.write.jdbc(url=jdbc_url, table="popular_bikes", mode="append", properties=connection_properties)
    def write_to_postgres_avg_duration(df, epoch_id):
        df.write.jdbc(url=jdbc_url, table="avg_duration", mode="append", properties=connection_properties)

    # Start the write stream queries
    popular_stations_write_stream = popular_stations \
        .writeStream \
        .trigger(processingTime='2 seconds') \
        .outputMode("complete") \
        .option("checkpointLocation", "/tmp/checkpoint1") \
        .foreachBatch(write_to_postgres_pop_sation) \
        .start()

    popular_bikes_write_stream = popular_bikes \
        .writeStream \
        .trigger(processingTime='2 seconds') \
        .outputMode("complete") \
        .option("checkpointLocation", "/tmp/checkpoint2") \
        .foreachBatch(write_to_postgres_popular_bikes) \
        .start()
    avg_duration_write_stream = avg_duration \
        .writeStream \
        .trigger(processingTime='2 seconds') \
        .outputMode("complete") \
        .option("checkpointLocation", "/tmp/checkpoint5") \
        .foreachBatch(write_to_postgres_avg_duration) \
        .start()

#mqtt
def insert_data(data):
    cur = conn.cursor()
    cur.execute("INSERT INTO mqtt_msg (msg) VALUES (%s)", (data,))
    conn.commit()
    cur.close()



def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker")
    client.subscribe("esp32")

def on_message(client, userdata, msg):
    data = msg.payload.decode()
    print(data)
    insert_data(data)
    with open("/home/gharsallah/Downloads/es.csv", "a") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow([data])

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("test.mosquitto.org", 1883, 60)

client.loop_forever()

spark.streams.awaitAnyTermination()
