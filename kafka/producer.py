from kafka import KafkaProducer
import pandas as pd
import json
import time
from hdfs import InsecureClient
import hashlib

# Kết nối đến HDFS
hdfs_client = InsecureClient('hdfs://localhost:9000', user='root')

# # Đường dẫn tới file trên HDFS
hdfs_path = '/dataFolder/final_data.csv'

# # Đọc dữ liệu từ HDFS
# with hdfs_client.read(hdfs_path, encoding='utf-8') as reader:
#     data = pd.read_csv(reader)

dataa = pd.read_csv('final_data.csv')

# Khởi tạo Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9093',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Set để lưu trữ các mã băm của các bản ghi đã gửi đi
sent_hashes = set()

# Gửi dữ liệu từng dòng vào Kafka topic theo từng nhóm 10 bản ghi mỗi 5 giây
batch_size = 50
for start in range(0, len(dataa), batch_size):
    batch = dataa[start:start + batch_size]
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
            "describe": row['describe']
        }
        producer.send('real_estate_data', record)
        
        # Thêm mã băm vào set của các mã băm đã gửi
        sent_hashes.add(record_hash)
    time.sleep(5)  # Thêm độ trễ 10 giây giữa mỗi nhóm


# ----------------------------------------------------------------
# from kafka import KafkaProducer
# import json
# import time
# from pyspark.sql import SparkSession

# # Tạo SparkSession
# spark = SparkSession.builder \
#     .appName("HDFS Read CSV") \
#     .getOrCreate()

# # Đường dẫn tới file trên HDFS
# hdfs_path = 'hdfs://localhost:9000/dataFolder/final_data.csv'

# # Đọc dữ liệu từ HDFS
# data = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# # Chuyển đổi DataFrame của Spark sang Pandas DataFrame
# pandas_df = data.toPandas()

# # Khởi tạo Kafka Producer
# producer = KafkaProducer(
#     bootstrap_servers='localhost:9093',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# # Gửi dữ liệu từng dòng vào Kafka topic theo từng nhóm 10 bản ghi mỗi 5 giây
# batch_size = 10
# for start in range(0, len(pandas_df), batch_size):
#     batch = pandas_df[start:start + batch_size]
#     for index, row in batch.iterrows():
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
#         producer.send('real_estate_data', record)
#     time.sleep(10)  # Thêm độ trễ 5 giây giữa mỗi nhóm

# # Dừng SparkSession sau khi hoàn thành
# spark.stop()


#----------------------------------------------------------------
#file sẽ dc sử dụng
# from kafka import KafkaProducer
# import pandas as pd
# import json
# import time
# from hdfs import InsecureClient

# # Kết nối đến HDFS
# hdfs_client = InsecureClient('http://172.18.0.5:9000', user='root')

# # # Đường dẫn tới file trên HDFS
# hdfs_path = '/dataFolder/final_data.csv'

# # # Đọc dữ liệu từ HDFS
# # with hdfs_client.read(hdfs_path, encoding='utf-8') as reader:
# #     data = pd.read_csv(reader)

# dataa = pd.read_csv('final_data.csv')
# # Khởi tạo Kafka Producer
# producer = KafkaProducer(
#     bootstrap_servers='localhost:9093',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# # Gửi dữ liệu từng dòng vào Kafka topic theo từng nhóm 10 bản ghi mỗi 5 giây
# batch_size = 10
# for start in range(0, len(dataa), batch_size):
#     batch = dataa[start:start + batch_size]
#     for index, row in batch.iterrows():
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
#         producer.send('real_estate_data', record)
#     time.sleep(10)  # Thêm độ trễ 5 giây giữa mỗi nhóm


# --------------------------------------------------------------------------------

# from kafka import KafkaProducer
# import pandas as pd
# import json
# import time

# # Đọc dữ liệu từ file CSV
# data = pd.read_csv('real_estate_data.csv')

# # Khởi tạo Kafka Producer
# producer = KafkaProducer(
#     bootstrap_servers='localhost:9093',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# # Gửi dữ liệu từng dòng vào Kafka topic
# for index, row in data.iterrows():
#     record = {
#         "link": row['link'],
#         "title": row['title'],
#         "estate_type": row['estate_type'],
#         "province": row['province'],
#         "district": row['district'],
#         "ward": row['ward'],
#         "price": row['price'],
#         "square": row['square'],
#         "post_date": row['post_date'],
#         "describe": row['describe']
#     }
#     producer.send('real_estate_data', record)
#     time.sleep(0.5)  # Thêm độ trễ để tránh quá tải Kafka

# producer.flush()
# producer.close()


# # from kafka import KafkaProducer
# # import json
# # import time
# # # import requests

# # # Thay đổi cổng kết nối Kafka thành 9093
# # producer = KafkaProducer(bootstrap_servers='localhost:9093', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# # data = [
# #     {"property_id": 1, "price": 300000, "location": "City A"},
# #     {"property_id": 2, "price": 450000, "location": "City B"},
# #     {"property_id": 3, "price": 200000, "location": "City C"},
# #     {"property_id": 4, "price": 550000, "location": "City D"},
# #     {"property_id": 5, "price": 200000, "location": "City E"},
# #     {"property_id": 6, "price": 550000, "location": "City F"},
# #     {"property_id": 7, "price": 200000, "location": "City G"},
# #     {"property_id": 8, "price": 550000, "location": "City H"},
# #     {"property_id": 9, "price": 250000, "location": "City H"},
# #     {"property_id": 10, "price": 250000, "location": "City H"},
# #     # Add more data as needed
# # ]

# # while True:
# #     for record in data:
# #         producer.send('real_estate_dataa', record)
# #         print(f'Sent: {record}')
# #     time.sleep(5)

# # # from kafka import KafkaProducer
# # # import json
# # # import time
# # # import requests

# # # # Thay đổi cổng kết nối Kafka thành 9093
# # # producer = KafkaProducer(bootstrap_servers='localhost:9093', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# # # while True:
# # #     response = requests.get('https://api.example.com/real-estate-data')
# # #     data = response.json()

# # #     for record in data:
# # #         producer.send('real_estate_data', record)
# # #         print(f'Sent: {record}')
    
# # #     time.sleep(3600)  # Đợi 1 giờ trước khi thu thập dữ liệu mới



# from kafka import KafkaProducer
# import pandas as pd
# import json
# import time

# # Đọc dữ liệu từ file CSV
# data = pd.read_csv('final_data.csv')

# # Khởi tạo Kafka Producer
# producer = KafkaProducer(
#     bootstrap_servers='localhost:9093',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# # Gửi dữ liệu từng dòng vào Kafka topic theo từng bản ghi mỗi 10 giây
# index = 0
# while index < len(data):
#     record = {
#         "link": data.at[index, 'link'],
#         "title": data.at[index, 'title'],
#         "estate_type": data.at[index, 'estate_type'],
#         "province": data.at[index, 'province'],
#         "district": data.at[index, 'district'],
#         "ward": data.at[index, 'ward'],
#         "price": data.at[index, 'price'],
#         "square": data.at[index, 'square'],
#         "post_date": data.at[index, 'post_date'],
#         "describe": data.at[index, 'describe']
#     }
#     producer.send('real_estate_data2', record)
#     index += 1
#     time.sleep(10)
