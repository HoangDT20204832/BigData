from kafka import KafkaProducer
import json
import time
import config

# Load configuration
app_config = config.Config(
    elasticsearch_host="localhost",
    elasticsearch_port="9200",
    elasticsearch_input_json="yes",
    elasticsearch_nodes_wan_only="true",
    hdfs_namenode="hdfs://localhost:9000"
)

# Tạo SparkSession
spark = app_config.initialize_spark_session("HDFS Read CSV")

# Đường dẫn tới file trên HDFS
hdfs_path = f"{app_config.get_hdfs_namenode()}/data1/final_data.csv"

# Đọc dữ liệu từ HDFS
data = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# Chuyển đổi DataFrame của Spark sang Pandas DataFrame
pandas_df = data.toPandas()

# Khởi tạo Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9093',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Gửi dữ liệu từng dòng vào Kafka topic theo từng nhóm 10 bản ghi mỗi 10 giây
batch_size = 10
for start in range(0, len(pandas_df), batch_size):
    batch = pandas_df[start:start + batch_size]
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
    time.sleep(10)  # Thêm độ trễ 10 giây giữa mỗi nhóm

# Dừng SparkSession sau khi hoàn thành
spark.stop()
