
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, min, max, year
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch

# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("RealEstateAnalysis") \
    .getOrCreate()
# spark = SparkSession.builder \
#     .appName("RealEstateAnalysis") \
#     .config("spark.master", "local") \
#     .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
#     .getOrCreate()

# Khởi tạo Elasticsearch
es = Elasticsearch(['http://localhost:9200'])

# Định nghĩa schema cho dữ liệu bất động sản
schema = StructType([
    StructField("link", StringType(), True),
    StructField("title", StringType(), True),
    StructField("estate_type", StringType(), True),
    StructField("province", StringType(), True),
    StructField("district", StringType(), True),
    StructField("ward", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("square", FloatType(), True),
    StructField("post_date", StringType(), True),
    StructField("describe", StringType(), True)
])

# Hàm để xử lý và lưu dữ liệu vào Elasticsearch
def process_and_save_to_es(df, epoch_id):
    # Tính giá trung bình và diện tích trung bình theo tỉnh/thành phố
    province_stats = df.groupBy("province").agg(
        avg("price").alias("avg_price"),
        count("price").alias("count_listings"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        avg("square").alias("avg_square")
    )
    
    # Tính giá trung bình và diện tích trung bình theo loại bất động sản
    estate_type_stats = df.groupBy("estate_type").agg(
        avg("price").alias("avg_price"),
        avg("square").alias("avg_square"),
        count("estate_type").alias("count_estate_type")
    )

    # Tỷ lệ các loại bất động sản
    estate_type_ratio = df.groupBy("estate_type").agg(
        count("estate_type").alias("count_estate_type")
    )
    total_estates = df.count()
    estate_type_ratio = estate_type_ratio.withColumn("ratio", col("count_estate_type") / total_estates)

    # Xu hướng giá theo năm
    df_with_year = df.withColumn("year", year("post_date"))
    price_trend = df_with_year.groupBy("year").agg(
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price")
    )
    
    # Mật độ bất động sản theo quận/huyện
    district_density = df.groupBy("district").agg(
        count("district").alias("count_listings")
    )

    # Chuyển đổi DataFrame thành dictionary để lưu vào Elasticsearch
    province_stats_dict = province_stats.toJSON().collect()
    estate_type_stats_dict = estate_type_stats.toJSON().collect()
    estate_type_ratio_dict = estate_type_ratio.toJSON().collect()
    price_trend_dict = price_trend.toJSON().collect()
    district_density_dict = district_density.toJSON().collect()
    records_dict = df.toJSON().collect() 
    # Lưu kết quả vào Elasticsearch
    for record in province_stats_dict:
        province_data = json.loads(record)
        province_name = province_data["province"]
        
        es.index(index="real_estate_analysis_province_stats", id=province_name, body={"doc": province_data, "doc_as_upsert": True})
    
    for record in estate_type_stats_dict:
        estate_type_data = json.loads(record)
        estate_type_name = estate_type_data["estate_type"]

        es.index(index="real_estate_analysis_estate_type_stats", id=estate_type_name, body={"doc": estate_type_data, "doc_as_upsert": True})

    for record in estate_type_ratio_dict:
        estate_type_ratio_data = json.loads(record)
        estate_type_name = estate_type_ratio_data["estate_type"]
        
        es.index(index="real_estate_analysis_estate_type_ratio", id=estate_type_name, body={"doc": estate_type_ratio_data, "doc_as_upsert": True})

    # for record in price_trend_dict:
    #     price_trend_data = json.loads(record)
    #     year_value = price_trend_data["year"]
        
    #     es.index(index="real_estate_analysis_price_trend", id=year_value, body={"doc": price_trend_data, "doc_as_upsert": True})

    for record in district_density_dict:
        district_density_data = json.loads(record)
        district_name = district_density_data["district"]
        
        es.index(index="real_estate_analysis_district_density", id=district_name, body={"doc": district_density_data, "doc_as_upsert": True})

    
    for record in records_dict:
    # Kiểm tra xem record đã tồn tại trong Elasticsearch hay không
        records_dict_data = json.loads(record)
        record_name = records_dict_data["title"]
    if not es.exists(index="real_estate_analysis", id=record_name):
        # Nếu không tồn tại, thêm record vào Elasticsearch
        es.index(index="real_estate_analysis", body={"doc": json.loads(record), "doc_as_upsert": True})

# Tạo Kafka Consumer
consumer = KafkaConsumer(
    'real_estate_data',
    bootstrap_servers='localhost:9093',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Nhận dữ liệu từ Kafka và chuyển thành DataFrame của Spark
data = []
for message in consumer:
    record = message.value
    data.append(record)
    
    # Khi dữ liệu đạt đến một số lượng nhất định, tiến hành phân tích và lưu vào Elasticsearch
    if len(data) >= 5:  # Số lượng có thể thay đổi tùy thuộc vào yêu cầu
        df = spark.createDataFrame(data, schema=schema)
        
        # Áp dụng schema và chuyển đổi thành DataFrame
        df = df.withColumn("price", col("price").cast(FloatType())) \
               .withColumn("square", col("square").cast(FloatType())) \
               .withColumn("post_date", col("post_date").cast(StringType()))
        
        # Gọi hàm xử lý và lưu dữ liệu
        process_and_save_to_es(df, None)
        
        # Xóa dữ liệu cũ sau khi xử lý
        # data.clear()

#----------------------------------------------------------------

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, avg, count, min, max, year
# from pyspark.sql.types import StructType, StructField, StringType, FloatType
# from kafka import KafkaConsumer
# import json
# from elasticsearch import Elasticsearch

# # Khởi tạo Spark session
# spark = SparkSession.builder \
#     .appName("RealEstateAnalysis") \
#     .getOrCreate()

# # Khởi tạo Elasticsearch
# es = Elasticsearch(['http://localhost:9200'])

# # Định nghĩa schema cho dữ liệu bất động sản
# schema = StructType([
#     StructField("link", StringType(), True),
#     StructField("title", StringType(), True),
#     StructField("estate_type", StringType(), True),
#     StructField("province", StringType(), True),
#     StructField("district", StringType(), True),
#     StructField("ward", StringType(), True),
#     StructField("price", FloatType(), True),
#     StructField("square", FloatType(), True),
#     StructField("post_date", StringType(), True),
#     StructField("describe", StringType(), True)
# ])

# # Hàm để xử lý và lưu dữ liệu vào Elasticsearch
# def process_and_save_to_es(df, epoch_id):
#     # Tính giá trung bình và diện tích trung bình theo tỉnh/thành phố
#     province_stats = df.groupBy("province").agg(
#         avg("price").alias("avg_price"),
#         count("price").alias("count_listings"),
#         min("price").alias("min_price"),
#         max("price").alias("max_price"),
#         avg("square").alias("avg_square")
#     )
    
#     # Tính giá trung bình và diện tích trung bình theo loại bất động sản
#     estate_type_stats = df.groupBy("estate_type").agg(
#         avg("price").alias("avg_price"),
#         avg("square").alias("avg_square"),
#         count("estate_type").alias("count_estate_type")
#     )

#     # Tỷ lệ các loại bất động sản
#     estate_type_ratio = df.groupBy("estate_type").agg(
#         count("estate_type").alias("count_estate_type")
#     )
#     total_estates = df.count()
#     estate_type_ratio = estate_type_ratio.withColumn("ratio", col("count_estate_type") / total_estates)

#     # Xu hướng giá theo năm
#     df_with_year = df.withColumn("year", year("post_date"))
#     price_trend = df_with_year.groupBy("year").agg(
#         avg("price").alias("avg_price"),
#         min("price").alias("min_price"),
#         max("price").alias("max_price")
#     )
    
#     # Mật độ bất động sản theo quận/huyện
#     district_density = df.groupBy("district").agg(
#         count("district").alias("count_listings")
#     )

#     # Chuyển đổi DataFrame thành dictionary để lưu vào Elasticsearch
#     province_stats_dict = province_stats.toJSON().collect()
#     estate_type_stats_dict = estate_type_stats.toJSON().collect()
#     estate_type_ratio_dict = estate_type_ratio.toJSON().collect()
#     price_trend_dict = price_trend.toJSON().collect()
#     district_density_dict = district_density.toJSON().collect()

#     # Lưu kết quả vào Elasticsearch
#     for record in province_stats_dict:
#         province_data = json.loads(record)
#         province_name = province_data["province"]
        
#         # Tìm kiếm bản ghi tỉnh/thành phố trong Elasticsearch
#         existing_record = es.search(
#             index="real_estate_analysis_province_stats",
#             body={"query": {"match": {"province": province_name}}}
#         )
        
#         if existing_record["hits"]["total"]["value"] > 0:
#             # Nếu đã tồn tại, cập nhật bản ghi
#             existing_record_id = existing_record["hits"]["hits"][0]["_id"]
#             es.update(index="real_estate_analysis_province_stats", id=existing_record_id, body={"doc": province_data})
#         else:
#             # Nếu chưa tồn tại, thêm mới bản ghi
#             es.index(index="real_estate_analysis_province_stats", body=province_data)
    
#     for record in estate_type_stats_dict:
#         estate_type_data = json.loads(record)
#         estate_type_name = estate_type_data["estate_type"]
        
#         # Tìm kiếm bản ghi loại bất động sản trong Elasticsearch
#         existing_record = es.search(
#             index="real_estate_analysis_estate_type_stats",
#             body={"query": {"match": {"estate_type": estate_type_name}}}
#         )
        
#         if existing_record["hits"]["total"]["value"] > 0:
#             # Nếu đã tồn tại, cập nhật bản ghi
#             existing_record_id = existing_record["hits"]["hits"][0]["_id"]
#             es.update(index="real_estate_analysis_estate_type_stats", id=existing_record_id, body={"doc": estate_type_data})
#         else:
#             # Nếu chưa tồn tại, thêm mới bản ghi
#             es.index(index="real_estate_analysis_estate_type_stats", body=estate_type_data)

#     for record in estate_type_ratio_dict:
#         estate_type_ratio_data = json.loads(record)
#         estate_type_name = estate_type_ratio_data["estate_type"]
        
#         # Tìm kiếm bản ghi loại bất động sản trong Elasticsearch
#         existing_record = es.search(
#             index="real_estate_analysis_estate_type_ratio",
#             body={"query": {"match": {"estate_type": estate_type_name}}}
#         )
        
#         if existing_record["hits"]["total"]["value"] > 0:
#             # Nếu đã tồn tại, cập nhật bản ghi
#             existing_record_id = existing_record["hits"]["hits"][0]["_id"]
#             es.update(index="real_estate_analysis_estate_type_ratio", id=existing_record_id, body={"doc": estate_type_ratio_data})
#         else:
#             # Nếu chưa tồn tại, thêm mới bản ghi
#             es.index(index="real_estate_analysis_estate_type_ratio", body=estate_type_ratio_data)

#     # for record in price_trend_dict:
#     #     price_trend_data = json.loads(record)
#     #     year_value = price_trend_data["year"]
        
#     #     # Tìm kiếm bản ghi xu hướng giá theo năm trong Elasticsearch
#     #     existing_record = es.search(
#     #         index="real_estate_analysis_price_trend",
#     #         body={"query": {"match": {"year": year_value}}}
#     #     )
        
#     #     if existing_record["hits"]["total"]["value"] > 0:
#     #         # Nếu đã tồn tại, cập nhật bản ghi
#     #         existing_record_id = existing_record["hits"]["hits"][0]["_id"]
#     #         es.update(index="real_estate_analysis_price_trend", id=existing_record_id, body={"doc": price_trend_data})
#     #     else:
#     #         # Nếu chưa tồn tại, thêm mới bản ghi
#     #         es.index(index="real_estate_analysis_price_trend", body=price_trend_data)

#     for record in district_density_dict:
#         district_density_data = json.loads(record)
#         district_name = district_density_data["district"]
        
#         # Tìm kiếm bản ghi mật độ bất động sản theo quận/huyện trong Elasticsearch
#         existing_record = es.search(
#             index="real_estate_analysis_district_density",
#             body={"query": {"match": {"district": district_name}}}
#         )
        
#         if existing_record["hits"]["total"]["value"] > 0:
#             # Nếu đã tồn tại, cập nhật bản ghi
#             existing_record_id = existing_record["hits"]["hits"][0]["_id"]
#             es.update(index="real_estate_analysis_district_density", id=existing_record_id, body={"doc": district_density_data})
#         else:
#             # Nếu chưa tồn tại, thêm mới bản ghi
#             es.index(index="real_estate_analysis_district_density", body=district_density_data)

#     records = df.toJSON().collect()
#     for record in records:
#         es.index(index="real_estate_analysissss", body=json.loads(record))
# # Tạo Kafka Consumer
# consumer = KafkaConsumer(
#     'real_estate_data',
#     bootstrap_servers='localhost:9093',
#     value_deserializer=lambda m: json.loads(m.decode('utf-8'))
# )

# # Nhận dữ liệu từ Kafka và chuyển thành DataFrame của Spark
# data = []
# for message in consumer:
#     record = message.value
#     data.append(record)
    
#     # Khi dữ liệu đạt đến một số lượng nhất định, tiến hành phân tích và lưu vào Elasticsearch
#     if len(data) >= 10:  # Số lượng có thể thay đổi tùy thuộc vào yêu cầu
#         df = spark.createDataFrame(data, schema=schema)
        
#         # Áp dụng schema và chuyển đổi thành DataFrame
#         df = df.withColumn("price", col("price").cast(FloatType())) \
#                .withColumn("square", col("square").cast(FloatType())) \
#                .withColumn("post_date", col("post_date").cast(StringType()))
        
#         # Gọi hàm xử lý và lưu dữ liệu
#         process_and_save_to_es(df, None)
        
#         # Xóa dữ liệu cũ sau khi xử lý
#         data.clear()


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, avg, count, min, max, year
# from pyspark.sql.types import StructType, StructField, StringType, FloatType
# from kafka import KafkaConsumer
# import json
# from elasticsearch import Elasticsearch

# # Khởi tạo Spark session
# spark = SparkSession.builder \
#     .appName("RealEstateAnalysis") \
#     .getOrCreate()

# # Khởi tạo Elasticsearch
# es = Elasticsearch(['http://localhost:9200'])

# # Định nghĩa schema cho dữ liệu bất động sản
# schema = StructType([
#     StructField("link", StringType(), True),
#     StructField("title", StringType(), True),
#     StructField("estate_type", StringType(), True),
#     StructField("province", StringType(), True),
#     StructField("district", StringType(), True),
#     StructField("ward", StringType(), True),
#     StructField("price", FloatType(), True),
#     StructField("square", FloatType(), True),
#     StructField("post_date", StringType(), True),
#     StructField("describe", StringType(), True)
# ])

# # Hàm để xử lý và lưu dữ liệu vào Elasticsearch
# def process_and_save_to_es(df, epoch_id):
#     # Tính giá trung bình và diện tích trung bình theo tỉnh/thành phố
#     province_stats = df.groupBy("province").agg(
#         avg("price").alias("avg_price"),
#         count("price").alias("count_listings"),
#         min("price").alias("min_price"),
#         max("price").alias("max_price"),
#         avg("square").alias("avg_square")
#     )
    
#     # Tính giá trung bình và diện tích trung bình theo loại bất động sản
#     estate_type_stats = df.groupBy("estate_type").agg(
#         avg("price").alias("avg_price"),
#         avg("square").alias("avg_square"),
#         count("estate_type").alias("count_estate_type")
#     )

#     # Tỷ lệ các loại bất động sản
#     estate_type_ratio = df.groupBy("estate_type").agg(
#         count("estate_type").alias("count_estate_type")
#     )
#     total_estates = df.count()
#     estate_type_ratio = estate_type_ratio.withColumn("ratio", col("count_estate_type") / total_estates)

#     # Xu hướng giá theo năm
#     df_with_year = df.withColumn("year", year("post_date"))
#     price_trend = df_with_year.groupBy("year").agg(
#         avg("price").alias("avg_price"),
#         min("price").alias("min_price"),
#         max("price").alias("max_price")
#     )
    
#     # Mật độ bất động sản theo quận/huyện
#     district_density = df.groupBy("district").agg(
#         count("district").alias("count_listings")
#     )

#     # Chuyển đổi DataFrame thành dictionary để lưu vào Elasticsearch
#     province_stats_dict = province_stats.toJSON().collect()
#     estate_type_stats_dict = estate_type_stats.toJSON().collect()
#     estate_type_ratio_dict = estate_type_ratio.toJSON().collect()
#     price_trend_dict = price_trend.toJSON().collect()
#     district_density_dict = district_density.toJSON().collect()

#     # Lưu kết quả vào Elasticsearch
#     for record in province_stats_dict:
#         es.index(index="real_estate_analysis_province_stats", body=json.loads(record))
    
#     for record in estate_type_stats_dict:
#         es.index(index="real_estate_analysis_estate_type_stats", body=json.loads(record))
    
#     for record in estate_type_ratio_dict:
#         es.index(index="real_estate_analysis_estate_type_ratio", body=json.loads(record))
    
#     for record in price_trend_dict:
#         es.index(index="real_estate_analysis_price_trend", body=json.loads(record))
    
#     for record in district_density_dict:
#         es.index(index="real_estate_analysis_district_density", body=json.loads(record))

#     records = df.toJSON().collect()
#     for record in records:
#         es.index(index="real_estate_analysissss", body=json.loads(record))
# # Tạo Kafka Consumer
# consumer = KafkaConsumer(
#     'real_estate_dataa',
#     bootstrap_servers='localhost:9093',
#     value_deserializer=lambda m: json.loads(m.decode('utf-8'))
# )

# # Nhận dữ liệu từ Kafka và chuyển thành DataFrame của Spark
# data = []
# for message in consumer:
#     record = message.value
#     data.append(record)
    
#     # Khi dữ liệu đạt đến một số lượng nhất định, tiến hành phân tích và lưu vào Elasticsearch
#     if len(data) >= 10:  # Số lượng có thể thay đổi tùy thuộc vào yêu cầu
#         df = spark.createDataFrame(data, schema=schema)
        
#         # Áp dụng schema và chuyển đổi thành DataFrame
#         df = df.withColumn("price", col("price").cast(FloatType())) \
#                .withColumn("square", col("square").cast(FloatType())) \
#                .withColumn("post_date", col("post_date").cast(StringType()))
        
#         # Gọi hàm xử lý và lưu dữ liệu
#         process_and_save_to_es(df, None)
        
#         # Xóa dữ liệu cũ sau khi xử lý
#         data.clear()

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from pyspark.sql.types import StructType, StructField, StringType, FloatType
# from kafka import KafkaConsumer
# import json
# from elasticsearch import Elasticsearch

# # Khởi tạo Elasticsearch
# es = Elasticsearch(['http://localhost:9200'])

# # Định nghĩa schema cho dữ liệu bất động sản
# schema = StructType([
#     StructField("link", StringType(), True),
#     StructField("title", StringType(), True),
#     StructField("estate_type", StringType(), True),
#     StructField("province", StringType(), True),
#     StructField("district", StringType(), True),
#     StructField("ward", StringType(), True),
#     StructField("price", FloatType(), True),
#     StructField("square", FloatType(), True),
#     StructField("post_date", StringType(), True),
#     StructField("describe", StringType(), True)
# ])

# # Khởi tạo Spark session
# spark = SparkSession.builder \
#     .appName("RealEstateAnalysis") \
#     .getOrCreate()

# # Hàm để xử lý và lưu dữ liệu vào Elasticsearch
# def process_and_save_to_es(df, epoch_id):
#     # Thực hiện các phép tính và phân tích tại đây, ví dụ: tính giá trung bình theo location
#     avg_price_df = df.groupBy("province").agg({"price": "avg"}).withColumnRenamed("avg(price)", "avg_price")
    
#     # Chuyển đổi DataFrame thành dictionary để lưu vào Elasticsearch
#     avg_price_dict = avg_price_df.rdd.map(lambda row: row.asDict()).collect()
    
#     # Lưu kết quả vào Elasticsearch
#     for record in avg_price_dict:
#         es.index(index="real_estate_analysisss", body=record)

# # Tạo Kafka Consumer
# consumer = KafkaConsumer(
#     'real_estate_data',
#     bootstrap_servers='localhost:9093',
#     value_deserializer=lambda m: json.loads(m.decode('utf-8'))
# )

# # Nhận dữ liệu từ Kafka và chuyển thành DataFrame của Spark
# data = []
# for message in consumer:
#     record = message.value
#     data.append(record)
    
#     # Khi dữ liệu đạt đến một số lượng nhất định, tiến hành phân tích và lưu vào Elasticsearch
#     if len(data) >= 10:  # Số lượng có thể thay đổi tùy thuộc vào yêu cầu
#         df = spark.createDataFrame(data, schema=schema)
        
#         # Áp dụng schema và chuyển đổi thành DataFrame
#         df = df.withColumn("price", col("price").cast(FloatType())) \
#                .withColumn("square", col("square").cast(FloatType())) \
#                .withColumn("post_date", col("post_date").cast(StringType()))
        
#         # Gọi hàm xử lý và lưu dữ liệu
#         process_and_save_to_es(df, None)
        
#         # Xóa dữ liệu cũ sau khi xử lý
#         data.clear()

#----------------------------------------------------------------

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import avg, count, min, max, col, year, month, dayofmonth
# from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
# from kafka import KafkaConsumer
# import json
# from elasticsearch import Elasticsearch

# # Khởi tạo Elasticsearch
# es = Elasticsearch(['http://localhost:9200'])

# # Định nghĩa schema cho dữ liệu bất động sản
# schema = StructType([
#     StructField("link", StringType(), True),
#     StructField("title", StringType(), True),
#     StructField("estate_type", StringType(), True),
#     StructField("province", StringType(), True),
#     StructField("district", StringType(), True),
#     StructField("ward", StringType(), True),
#     StructField("price", FloatType(), True),
#     StructField("square", FloatType(), True),
#     StructField("post_date", StringType(), True),
#     StructField("describe", StringType(), True)
# ])

# # Khởi tạo Spark session
# spark = SparkSession.builder \
#     .appName("RealEstateAnalysis") \
#     .getOrCreate()

# # Hàm để xử lý và lưu dữ liệu vào Elasticsearch
# def process_and_save_to_es(df, epoch_id):

#     records = df.toJSON().collect()
#     for record in records:
#         es.index(index="real_estate_analysisss", body=json.loads(record))
#     # Tính giá trung bình và diện tích trung bình theo tỉnh/thành phố
#     province_stats = df.groupBy("province").agg(
#         avg("price").alias("avg_price"),
#         count("price").alias("count_listings"),
#         min("price").alias("min_price"),
#         max("price").alias("max_price"),
#         avg("square").alias("avg_square")
#     )
    
#     # Tính giá trung bình và diện tích trung bình theo loại bất động sản
#     estate_type_stats = df.groupBy("estate_type").agg(
#         avg("price").alias("avg_price"),
#         avg("square").alias("avg_square"),
#         count("estate_type").alias("count_estate_type")
#     )

#     # Tỷ lệ các loại bất động sản
#     estate_type_ratio = df.groupBy("estate_type").agg(
#         count("estate_type").alias("count_estate_type")
#     )
#     total_estates = df.count()
#     estate_type_ratio = estate_type_ratio.withColumn("ratio", col("count_estate_type") / total_estates)

#     # Xu hướng giá theo năm
#     df_with_year = df.withColumn("year", year("post_date"))
#     price_trend = df_with_year.groupBy("year").agg(
#         avg("price").alias("avg_price"),
#         min("price").alias("min_price"),
#         max("price").alias("max_price")
#     )
    
#     # Mật độ bất động sản theo quận/huyện
#     district_density = df.groupBy("district").agg(
#         count("district").alias("count_listings")
#     )


#     # Chuyển đổi DataFrame thành dictionary để lưu vào Elasticsearch
#     province_stats_dict = province_stats.rdd.map(lambda row: row.asDict()).collect()
#     estate_type_stats_dict = estate_type_stats.rdd.map(lambda row: row.asDict()).collect()
#     estate_type_ratio_dict = estate_type_ratio.rdd.map(lambda row: row.asDict()).collect()
#     price_trend_dict = price_trend.rdd.map(lambda row: row.asDict()).collect()
#     district_density_dict = district_density.rdd.map(lambda row: row.asDict()).collect()


#     # Lưu kết quả vào Elasticsearch
#     # for record in records:
#     #     es.index(index="real_estate_analysiss", body=json.loads(record))
#     for record in province_stats_dict:
#         es.index(index="real_estate_analysis_province_stats", body=record)
    
#     for record in estate_type_stats_dict:
#         es.index(index="real_estate_analysis_estate_type_stats", body=record)
    
#     for record in estate_type_ratio_dict:
#         es.index(index="real_estate_analysis_estate_type_ratio", body=record)
    
#     for record in price_trend_dict:
#         es.index(index="real_estate_analysis_price_trend", body=record)
    
#     for record in district_density_dict:
#         es.index(index="real_estate_analysis_district_density", body=record)

# # Tạo Kafka Consumer
# consumer = KafkaConsumer(
#     'real_estate_data',
#     bootstrap_servers='localhost:9093',
#     value_deserializer=lambda m: json.loads(m.decode('utf-8'))
# )

# # Nhận dữ liệu từ Kafka và chuyển thành RDD của Spark
# data = []
# for message in consumer:
#     record = message.value
#     data.append(record)
    
#     # Khi dữ liệu đạt đến một số lượng nhất định, tiến hành phân tích và lưu vào Elasticsearch
#     if len(data) >= 100:  # Số lượng có thể thay đổi tùy thuộc vào yêu cầu
#         df = spark.createDataFrame(data, schema=schema)
        
#         # Áp dụng schema và chuyển đổi thành DataFrame
#         df = df.withColumn("price", col("price").cast(FloatType())) \
#                .withColumn("square", col("square").cast(FloatType())) \
#                .withColumn("post_date", col("post_date").cast(StringType()))
        
#         # Gọi hàm xử lý và lưu dữ liệu
#         process_and_save_to_es(df, None)
        
#         # Xóa dữ liệu cũ sau khi xử lý
#         data.clear()

# -----------------------------------------------------------

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import avg, count, min, max, col, year, month, dayofmonth
# from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
# from kafka import KafkaConsumer
# import json
# from elasticsearch import Elasticsearch

# # Khởi tạo Elasticsearch
# es = Elasticsearch(['http://localhost:9200'])

# # Định nghĩa schema cho dữ liệu bất động sản
# schema = StructType([
#     StructField("link", StringType(), True),
#     StructField("title", StringType(), True),
#     StructField("estate_type", StringType(), True),
#     StructField("province", StringType(), True),
#     StructField("district", StringType(), True),
#     StructField("ward", StringType(), True),
#     StructField("price", FloatType(), True),
#     StructField("square", FloatType(), True),
#     StructField("post_date", StringType(), True),
#     StructField("describe", StringType(), True)
# ])

# # Khởi tạo Spark session
# spark = SparkSession.builder \
#     .appName("RealEstateAnalysis") \
#     .getOrCreate()

# # Hàm để xử lý và lưu dữ liệu vào Elasticsearch
# def process_and_save_to_es(df, epoch_id):

#     records = df.toJSON().collect()
#     for record in records:
#         es.index(index="real_estate_analysis2", body=json.loads(record))
#     # Tính giá trung bình và diện tích trung bình theo tỉnh/thành phố
#     province_stats = df.groupBy("province").agg(
#         avg("price").alias("avg_price"),
#         count("price").alias("count_listings"),
#         min("price").alias("min_price"),
#         max("price").alias("max_price"),
#         avg("square").alias("avg_square")
#     )
    
#     # Tính giá trung bình và diện tích trung bình theo loại bất động sản
#     estate_type_stats = df.groupBy("estate_type").agg(
#         avg("price").alias("avg_price"),
#         avg("square").alias("avg_square"),
#         count("estate_type").alias("count_estate_type")
#     )

#     # Tỷ lệ các loại bất động sản
#     estate_type_ratio = df.groupBy("estate_type").agg(
#         count("estate_type").alias("count_estate_type")
#     )
#     total_estates = df.count()
#     estate_type_ratio = estate_type_ratio.withColumn("ratio", col("count_estate_type") / total_estates)

#     # Xu hướng giá theo năm
#     df_with_year = df.withColumn("year", year("post_date"))
#     price_trend = df_with_year.groupBy("year").agg(
#         avg("price").alias("avg_price"),
#         min("price").alias("min_price"),
#         max("price").alias("max_price")
#     )
    
#     # Mật độ bất động sản theo quận/huyện
#     district_density = df.groupBy("district").agg(
#         count("district").alias("count_listings")
#     )


#     # Chuyển đổi DataFrame thành dictionary để lưu vào Elasticsearch
#     province_stats_dict = province_stats.rdd.map(lambda row: row.asDict()).collect()
#     estate_type_stats_dict = estate_type_stats.rdd.map(lambda row: row.asDict()).collect()
#     estate_type_ratio_dict = estate_type_ratio.rdd.map(lambda row: row.asDict()).collect()
#     price_trend_dict = price_trend.rdd.map(lambda row: row.asDict()).collect()
#     district_density_dict = district_density.rdd.map(lambda row: row.asDict()).collect()


#     # Lưu kết quả vào Elasticsearch
#     for record in df:
#         es.index(index="real_estate_analysiss", body=record)
#     for record in province_stats_dict:
#         es.index(index="real_estate_analysis_province_stats", body=record)
    
#     for record in estate_type_stats_dict:
#         es.index(index="real_estate_analysis_estate_type_stats", body=record)
    
#     for record in estate_type_ratio_dict:
#         es.index(index="real_estate_analysis_estate_type_ratio", body=record)
    
#     for record in price_trend_dict:
#         es.index(index="real_estate_analysis_price_trend", body=record)
    
#     for record in district_density_dict:
#         es.index(index="real_estate_analysis_district_density", body=record)

# # Tạo Kafka Consumer
# consumer = KafkaConsumer(
#     'real_estate_data',
#     bootstrap_servers='localhost:9093',
#     value_deserializer=lambda m: json.loads(m.decode('utf-8'))
# )

# # Nhận dữ liệu từ Kafka và chuyển thành RDD của Spark
# data = []
# for message in consumer:
#     record = message.value
#     data.append(record)
    
#     # Khi dữ liệu đạt đến một số lượng nhất định, tiến hành phân tích và lưu vào Elasticsearch
#     if len(data) >= 100:  # Số lượng có thể thay đổi tùy thuộc vào yêu cầu
#         df = spark.createDataFrame(data, schema=schema)
        
#         # Áp dụng schema và chuyển đổi thành DataFrame
#         df = df.withColumn("price", col("price").cast(FloatType())) \
#                .withColumn("square", col("square").cast(FloatType())) \
#                .withColumn("post_date", col("post_date").cast(StringType()))
        
#         # Gọi hàm xử lý và lưu dữ liệu
#         process_and_save_to_es(df, None)
        
#         # Xóa dữ liệu cũ sau khi xử lý
#         data.clear()

# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import col
# # from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# # from kafka import KafkaConsumer
# # import json
# # from elasticsearch import Elasticsearch

# # # Khởi tạo Elasticsearch
# # es = Elasticsearch(['http://localhost:9200'])

# # # Định nghĩa schema cho dữ liệu bất động sản
# # schema = StructType([
# #     StructField("property_id", IntegerType(), True),
# #     StructField("price", IntegerType(), True),
# #     StructField("location", StringType(), True)
# # ])

# # # Khởi tạo Spark session
# # spark = SparkSession.builder \
# #     .appName("RealEstateAnalysis") \
# #     .getOrCreate()

# # # Hàm để xử lý và lưu dữ liệu vào Elasticsearch
# # def process_and_save_to_es(df, epoch_id):
# #     # Thực hiện các phép tính và phân tích tại đây, ví dụ: tính giá trung bình theo location
# #     avg_price_df = df.groupBy("location").agg({"price": "avg"}).withColumnRenamed("avg(price)", "avg_price")
    
# #     # Chuyển đổi DataFrame thành dictionary để lưu vào Elasticsearch
# #     avg_price_dict = avg_price_df.rdd.map(lambda row: row.asDict()).collect()
    
# #     # Lưu kết quả vào Elasticsearch
# #     for record in avg_price_dict:
# #         es.index(index="real_estate_analysis1", body=record)

# # # Tạo Kafka Consumer
# # consumer = KafkaConsumer(
# #     'real_estate_dataa',
# #     bootstrap_servers='localhost:9093',
# #     value_deserializer=lambda m: json.loads(m.decode('utf-8'))
# # )

# # # Nhận dữ liệu từ Kafka và chuyển thành RDD của Spark
# # for message in consumer:
# #     record = message.value
# #     df = spark.createDataFrame([record], schema=schema)
    
# #     # Áp dụng schema và chuyển đổi thành DataFrame
# #     df = df.withColumn("property_id", col("property_id").cast(IntegerType())) \
# #            .withColumn("price", col("price").cast(IntegerType())) \
# #            .withColumn("location", col("location").cast(StringType()))
    
# #     # Gọi hàm xử lý và lưu dữ liệu
# #     process_and_save_to_es(df, None)


#----------------------------------------------------------------
# # # from pyspark.sql import SparkSession
# # # from pyspark.sql.functions import avg, col
# # # from pyspark.ml.feature import VectorAssembler
# # # from pyspark.ml.regression import LinearRegression

# # # # Khởi tạo Spark Session
# # # spark = SparkSession.builder.appName('RealEstateAnalysis').getOrCreate()

# # # # Đọc dữ liệu từ HDFS
# # # df = spark.read.json('hdfs://hadoop-namenode:9000/path/to/your/real_estate_data.json')

# # # # Tính giá trung bình theo khu vực
# # # avg_price = df.groupBy('location').agg(avg('price').alias('avg_price'))
# # # avg_price.show()

# # # # Huấn luyện mô hình dự báo giá bất động sản
# # # assembler = VectorAssembler(inputCols=['feature1', 'feature2', 'feature3'], outputCol='features')
# # # data = assembler.transform(df)
# # # data = data.select('features', col('price').alias('label'))

# # # # Chia dữ liệu thành tập huấn luyện và tập kiểm tra
# # # train_data, test_data = data.randomSplit([0.8, 0.2])

# # # # Huấn luyện mô hình hồi quy tuyến tính
# # # lr = LinearRegression()
# # # model = lr.fit(train_data)

# # # # Đánh giá mô hình
# # # test_results = model.evaluate(test_data)
# # # print(f'RMSE: {test_results.rootMeanSquaredError}')
# # # print(f'R2: {test_results.r2}')

# # # # Lưu kết quả vào Elasticsearch
# # # avg_price.write.format('org.elasticsearch.spark.sql').option('es.resource', 'real_estate/analysis').save()
