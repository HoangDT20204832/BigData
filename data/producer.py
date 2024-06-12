from kafka import KafkaProducer
import pandas as pd
import json
import time
from hdfs import InsecureClient

# Kết nối đến HDFS
hdfs_host = 'http://namenode:9870'
hdfs_path = '/data/real_estate_data.csv'
hdfs_client = InsecureClient(hdfs_host, user='root')

# Đọc dữ liệu từ HDFS
with hdfs_client.read(hdfs_path, encoding='utf-8') as reader:
    data = pd.read_csv(reader)

# Khởi tạo Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='http://localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Gửi dữ liệu từng dòng vào Kafka topic theo từng nhóm 10 bản ghi mỗi 10 giây
batch_size = 10
for start in range(0, len(data), batch_size):
    batch = data[start:start + batch_size]
    for index, row in batch.iterrows():
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
    # time.sleep(10)  # Thêm độ trễ 10 giây giữa mỗi nhóm

print("Data sent to Kafka successfully.")

# from kafka import KafkaProducer
# import pandas as pd
# import json
# import time
# from hdfs import InsecureClient

# # Kết nối đến HDFS
# hdfs_client = InsecureClient('http://localhost:9870', user='root')

# # Đường dẫn tới file trên HDFS
# hdfs_path = '/data/real_estate_data.csv'

# # Đọc dữ liệu từ HDFS
# with hdfs_client.read(hdfs_path, encoding='utf-8') as reader:
#     data = pd.read_csv(reader)

# # data = pd.read_csv('real_estate_data.csv')
# # Khởi tạo Kafka Producer
# producer = KafkaProducer(
#     bootstrap_servers='localhost:9093',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# # Gửi dữ liệu từng dòng vào Kafka topic theo từng nhóm 10 bản ghi mỗi 5 giây
# batch_size = 10
# for start in range(0, len(data), batch_size):
#     batch = data[start:start + batch_size]
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


