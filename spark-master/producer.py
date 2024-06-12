
# ----------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from kafka import KafkaProducer
import json
import os
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HDFSReader") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# HDFS and Kafka configurations
hdfs_host = 'hdfs://namenode:9000'
hdfs_path = '/dataFolder/final_data.csv'
kafka_bootstrap_servers = 'btl-bigdata-kafka-1:9092'
kafka_topic = 'real_estate_dataa'
state_file = 'sent_titles.json'

# Read data from HDFS and convert to DataFrame
df = spark.read.csv(hdfs_host + hdfs_path, header=True, inferSchema=True)

# Remove duplicates within the dataframe based on title
df_unique = df.dropDuplicates(subset=['title'])

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to send data to Kafka topic
def send_to_kafka(record):
    producer.send(kafka_topic, record)

# Load sent titles from the state file
if os.path.exists(state_file):
    with open(state_file, 'r') as f:
        sent_titles = set(json.load(f))
else:
    sent_titles = set()

# Send unique records to Kafka
for row in df_unique.collect():
    title = row['title']
    if title not in sent_titles:
        record = {
            "link": row['link'],
            "title": title,
            "estate_type": row['estate_type'],
            "province": row['province'],
            "district": row['district'],
            "ward": row['ward'],
            "price": row['price'],
            "square": row['square'],
            "post_date": row['post_date'],
            "describe": row['describe']
        }
        send_to_kafka(record)
        sent_titles.add(title)
        # Sleep for a short duration to control the speed of sending
        time.sleep(0.2)

# Update sent titles state file
with open(state_file, 'w') as f:
    json.dump(list(sent_titles), f)

# Close Kafka producer
producer.close()

# Stop the Spark session
spark.stop()
# ----------------------------------------------------------------

# from pyspark.sql import SparkSession
# from kafka import KafkaProducer
# import json
# import time
# import os


# # Initialize Spark Session
# spark = SparkSession.builder \
#     .appName("HDFSReader") \
#     .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
#     .getOrCreate()

# # HDFS and Kafka configurations
# hdfs_host = 'hdfs://namenode:9000'
# hdfs_path = '/data1/final_data.csv'
# kafka_bootstrap_servers = 'btl-bigdata-kafka-1:9092'
# kafka_topic = 'real_estate_dataa'
# state_file = 'sent_titles.json'


# # Read data from HDFS and convert to DataFrame
# df = spark.read.csv(hdfs_host + hdfs_path, header=True, inferSchema=True)

# # Remove duplicates within the dataframe based on title
# df_unique = df.dropDuplicates(subset=['title'])

# # Initialize Kafka Producer
# producer = KafkaProducer(
#     bootstrap_servers=kafka_bootstrap_servers,
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# # Load sent titles from the state file
# if os.path.exists(state_file):
#     with open(state_file, 'r') as f:
#         sent_titles = set(json.load(f))
# else:
#     sent_titles = set()

# # Function to send data in batches to Kafka topic
# def send_batches_to_kafka(df, batch_size=50, sleep_time=5):
#     new_sent_titles = set()
    
#     # Use DataFrame's toLocalIterator to avoid collecting all data at once
#     data_iter = df.toLocalIterator()
    
#     batch = []
#     for row in data_iter:
#         title = row['title']
#         if title not in sent_titles and title not in new_sent_titles:
#             record = {
#                 "link": row['link'],
#                 "title": row['title'],
#                 "estate_type": row['estate_type'],
#                 "province": row['province'],
#                 "district": row['district'],
#                 "ward": row['ward'],
#                 "price": row['price'],
#                 "square": row['square'],
#                 "post_date": row['post_date'],
#                 "describe": row['describe']
#             }
#             batch.append(record)
#             new_sent_titles.add(title)

#         if len(batch) >= batch_size:
#             for record in batch:
#                 producer.send(kafka_topic, record)
#             producer.flush()  # Ensure all messages are sent immediately
#             time.sleep(sleep_time)  # Wait before sending the next batch
#             batch = []

#     # Send any remaining records in the last batch
#     if batch:
#         for record in batch:
#             producer.send(kafka_topic, record)
#         producer.flush()

#     # Update sent titles state file
#     sent_titles.update(new_sent_titles)
#     with open(state_file, 'w') as f:
#         json.dump(list(sent_titles), f)

# # Send unique records to Kafka
# send_batches_to_kafka(df_unique)

# print("Unique data sent to Kafka successfully.")

# # Stop the Spark session
# spark.stop()

# ----------------------------------------------------------------


# from pyspark.sql import SparkSession
# from kafka import KafkaProducer
# import json
# import time

# # Initialize Spark Session
# spark = SparkSession.builder \
#     .appName("HDFSReader") \
#     .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
#     .getOrCreate()

# # HDFS and Kafka configurations
# hdfs_host = 'hdfs://namenode:9000'
# hdfs_path = '/dataFolder/final_data.csv'
# kafka_bootstrap_servers = 'btl-bigdata-kafka-1:9092'
# kafka_topic = 'real_estate_data'

# # Read data from HDFS and convert to DataFrame
# df = spark.read.csv(hdfs_host + hdfs_path, header=True, inferSchema=True)

# # Remove duplicate titles
# df_unique = df.dropDuplicates(['title'])

# # Initialize Kafka Producer
# producer = KafkaProducer(
#     bootstrap_servers=kafka_bootstrap_servers,
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# # Function to send data in batches to Kafka topic
# def send_batches_to_kafka(df, batch_size=50, sleep_time=5):
#     data_iter = df.toLocalIterator()
#     row_count = df.count()  # Get the total number of rows in the DataFrame
#     for start in range(0, row_count, batch_size):
#         batch = []
#         for _ in range(batch_size):
#             try:
#                 row = next(data_iter)
#                 record = {
#                     "link": row['link'],
#                     "title": row['title'],
#                     "estate_type": row['estate_type'],
#                     "province": row['province'],
#                     "district": row['district'],
#                     "ward": row['ward'],
#                     "price": row['price'],
#                     "square": row['square'],
#                     "post_date": row['post_date'],
#                     "describe": row['describe']
#                 }
#                 batch.append(record)
#             except StopIteration:
#                 break

#         for record in batch:
#             producer.send(kafka_topic, record)
#         producer.flush()  # Ensure all messages are sent immediately
#         time.sleep(sleep_time)  # Wait before sending the next batch

# # Send unique records to Kafka
# send_batches_to_kafka(df_unique)

# print("Unique data sent to Kafka successfully.")

# # Stop the Spark session
# spark.stop()



# ----------------------------------------------------------------



# from pyspark.sql import SparkSession
# from kafka import KafkaProducer
# import json
# import time

# # Initialize Spark Session
# spark = SparkSession.builder \
#     .appName("HDFSReader") \
#     .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
#     .getOrCreate()

# # HDFS and Kafka configurations
# hdfs_host = 'hdfs://namenode:9000'
# hdfs_path = '/dataFolder/final_data.csv'
# kafka_bootstrap_servers = 'btl-bigdata-kafka-1:9092'  # Updated Kafka bootstrap servers address
# kafka_topic = 'real_estate_data'

# # Read data from HDFS and convert to DataFrame
# df = spark.read.csv(hdfs_host + hdfs_path, header=True, inferSchema=True)

# # Initialize Kafka Producer
# producer = KafkaProducer(
#     bootstrap_servers=kafka_bootstrap_servers,
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# # Send data in batches to Kafka topic
# batch_size = 50  # Increase batch size to 100
# data = df.collect()  # Collect all rows from DataFrame
# for start in range(0, len(data), batch_size):
#     batch = data[start:start + batch_size]
#     for row in batch:
#         record = {
#             "link": row['link'],
#             "title": row['title'],
#             "estate_type": row['estate_type'],
#             "province": row['province'],
#             "district": row['district'],
#             "ward": row['ward'],
#             "price": row['price'],
#             "square": row['square'],
#             "post_date": row['post_date'],
#             "describe": row['describe']
#         }
#         producer.send(kafka_topic, record)
#     producer.flush()  # Ensure all messages are sent immediately
#     time.sleep(5)  # Reduce sleep time to 1 second between batches

# print("Data sent to Kafka successfully.")

# # Stop the Spark session
# spark.stop()


