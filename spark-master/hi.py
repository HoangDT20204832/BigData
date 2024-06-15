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
