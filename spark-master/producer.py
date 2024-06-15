from kafka import KafkaProducer
import pandas as pd
import json
import time
from hdfs import InsecureClient
import hashlib

# Kết nối đến HDFS qua giao thức webhdfs
hdfs_client = InsecureClient('http://namenode:9870', user='root')

# Đường dẫn tới file trên HDFS
hdfs_path = '/dataFolder/final_data.csv'

# Đọc dữ liệu từ HDFS
with hdfs_client.read(hdfs_path, encoding='utf-8') as reader:
    data = pd.read_csv(reader)

# Khởi tạo Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='btl-bigdata-kafka-1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Set để lưu trữ các mã băm của các bản ghi đã gửi đi
sent_hashes = set()

# Gửi dữ liệu từng dòng vào Kafka topic theo từng nhóm 10 bản ghi mỗi 5 giây
batch_size = 50
for start in range(0, len(data), batch_size):
    batch = data[start:start + batch_size]
    for index, row in batch.iterrows():
        # Tạo mã băm cho bản ghi
        record_hash = hashlib.sha256(json.dumps(row.to_dict(), sort_keys=True).encode()).hexdigest()
        
        # Kiểm tra nếu mã băm đã tồn tại trong set thì bỏ qua
        if record_hash in sent_hashes:
            continue
        
        record = {
            "link": row['link'],
            "title": row['title'],
            "estate_type": row['estate_type'],
            "province": row['province'],
            "district": row['district'],
            "ward": row['ward'],
            "price": row['price'],
            "square": row['square'],
            "post_date": row['post_date'],
            "describe": row['describe'],
            "price/square": row['price/square']
        }
        producer.send('real_estate_data_', record)
        
        # Thêm mã băm vào set của các mã băm đã gửi
        sent_hashes.add(record_hash)
    time.sleep(5)  # Thêm độ trễ 5 giây giữa mỗi nhóm

# ----------------------------------------------------------------

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from kafka import KafkaProducer
# import json
# import os
# import time

# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("HDFSReader") \
#     .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
#     .getOrCreate()

# # HDFS and Kafka configurations
# hdfs_host = 'hdfs://namenode:9000'
# hdfs_path = '/dataFolder/final_data.csv'
# kafka_bootstrap_servers = 'btl-bigdata-kafka:9092'
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

# # Function to send data to Kafka topic
# def send_to_kafka(record):
#     producer.send(kafka_topic, record)

# # Load sent titles from the state file
# if os.path.exists(state_file):
#     with open(state_file, 'r') as f:
#         sent_titles = set(json.load(f))
# else:
#     sent_titles = set()

# # Send unique records to Kafka
# for row in df_unique.collect():
#     title = row['title']
#     if title not in sent_titles:
#         record = {
#             "link": row['link'],
#             "title": title,
#             "estate_type": row['estate_type'],
#             "province": row['province'],
#             "district": row['district'],
#             "ward": row['ward'],
#             "price": row['price'],
#             "square": row['square'],
#             "post_date": row['post_date'],
#             "describe": row['describe'],
#             "price/square": row['price/square']
#         }
#         send_to_kafka(record)
#         sent_titles.add(title)
#         # Sleep for a short duration to control the speed of sending
#         time.sleep(0.2)

# # Update sent titles state file
# with open(state_file, 'w') as f:
#     json.dump(list(sent_titles), f)

# # Close Kafka producer
# producer.close()

# # Stop the Spark session
# spark.stop()
