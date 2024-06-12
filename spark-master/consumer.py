from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, min, max, year
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch, helpers, exceptions
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HDFSReader") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Initialize Elasticsearch
es_host = 'http://btl-bigdata-elasticsearch-1:9200'
es = Elasticsearch([es_host])

# Check Elasticsearch connection
def check_es_connection():
    try:
        if es.ping():
            logger.info("Successfully connected to Elasticsearch")
        else:
            raise ValueError("Elasticsearch connection failed")
    except exceptions.ConnectionError as e:
        logger.error("Elasticsearch connection error: {}".format(e))
        raise e

# Define schema for real estate data
schema = StructType([
    StructField("link", StringType(), True),
    StructField("title", StringType(), True),
    StructField("estate_type", StringType(), True),
    StructField("province", StringType(), True),
    StructField("district", StringType(), True),
    StructField("ward", StringType(), True),
    StructField("price", StringType(), True),  # Change to StringType
    StructField("square", StringType(), True),  # Change to StringType
    StructField("post_date", StringType(), True),
    StructField("describe", StringType(), True)
])


# Process and save data to Elasticsearch
def process_and_save_to_es(df, epoch_id):
    logger.info("Processing batch...")

    # Convert columns to appropriate data types with error handling
    df = df.withColumn("price", col("price").cast("float")).filter(col("price").isNotNull()) \
           .withColumn("square", col("square").cast("float")).filter(col("square").isNotNull())

    # Calculate average price and square footage by province
    province_stats = df.groupBy("province").agg(
        avg("price").alias("avg_price"),
        count("price").alias("count_listings"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        avg("square").alias("avg_square")
    )

    # Calculate average price and square footage by estate type
    estate_type_stats = df.groupBy("estate_type").agg(
        avg("price").alias("avg_price"),
        avg("square").alias("avg_square"),
        count("estate_type").alias("count_estate_type")
    )

    # Estate type ratio
    estate_type_ratio = df.groupBy("estate_type").agg(
        count("estate_type").alias("count_estate_type")
    )
    total_estates = df.count()
    estate_type_ratio = estate_type_ratio.withColumn("ratio", col("count_estate_type") / total_estates)

    # Price trend by year
    df_with_year = df.withColumn("year", year("post_date"))
    price_trend = df_with_year.groupBy("year").agg(
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price")
    )
    
    # Real estate density by district
    district_density = df.groupBy("district").agg(
        count("district").alias("count_listings")
    )

    # Convert DataFrame to dictionary for indexing into Elasticsearch
    province_stats_dict = province_stats.toJSON().collect()
    estate_type_stats_dict = estate_type_stats.toJSON().collect()
    estate_type_ratio_dict = estate_type_ratio.toJSON().collect()
    price_trend_dict = price_trend.toJSON().collect()
    district_density_dict = district_density.toJSON().collect()
    records_dict = df.toJSON().collect() 

    # Save results to Elasticsearch using bulk API
    actions = []
    for record in province_stats_dict:
        province_data = json.loads(record)
        province_name = province_data["province"]
        actions.append({
            "_index": "real_estate_analysis_province_stats",
            "_id": province_name,
            "_source": {"doc": province_data, "doc_as_upsert": True}
        })
    
    for record in estate_type_stats_dict:
        estate_type_data = json.loads(record)
        estate_type_name = estate_type_data["estate_type"]
        actions.append({
            "_index": "real_estate_analysis_estate_type_stats",
            "_id": estate_type_name,
            "_source": {"doc": estate_type_data, "doc_as_upsert": True}
        })

    for record in estate_type_ratio_dict:
        estate_type_ratio_data = json.loads(record)
        estate_type_name = estate_type_ratio_data["estate_type"]
        actions.append({
            "_index": "real_estate_analysis_estate_type_ratio",
            "_id": estate_type_name,
            "_source": {"doc": estate_type_ratio_data, "doc_as_upsert": True}
        })

    for record in district_density_dict:
        district_density_data = json.loads(record)
        district_name = district_density_data["district"]
        actions.append({
            "_index": "real_estate_analysis_district_density",
            "_id": district_name,
            "_source": {"doc": district_density_data, "doc_as_upsert": True}
        })

    for record in records_dict:
        records_dict_data = json.loads(record)
        record_name = records_dict_data["link"]
        actions.append({
            "_index": "real_estate_analysis",
            "_id": record_name,
            "_source": {"doc": records_dict_data, "doc_as_upsert": True}
        })
    
    helpers.bulk(es, actions)
    logger.info("Batch processed and saved to Elasticsearch.")

    # update_statistics_from_all_records("real_estate_analysis_estate_type_stats")

# Create Kafka Consumer
consumer = KafkaConsumer(
    'real_estate_dataa',
    bootstrap_servers='btl-bigdata-kafka-1:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Receive data from Kafka and convert to Spark DataFrame
data = []
for message in consumer:
    record = message.value
    data.append(record)
    logger.info("Received record: {}".format(record))
    
    # When data reaches a certain amount, perform analysis and save to Elasticsearch
    if len(data) >= 10:  # Increase the number of records to process each time
        df = spark.createDataFrame(data, schema=schema)
        
        # Apply schema and convert to DataFrame
        df = df.withColumn("price", col("price").cast("float")).filter(col("price").isNotNull()) \
               .withColumn("square", col("square").cast("float")).filter(col("square").isNotNull()) \
               .withColumn("post_date", col("post_date").cast(StringType()))
        
        # Check Elasticsearch connection
        try:
            check_es_connection()
            # Call function to process and save data
            process_and_save_to_es(df, None)
        except Exception as e:
            logger.error("Error while processing data: {}".format(e))
        
        # Clear old data after processing
        # data = []  # Using assignment instead of clear method for compatibility



#----------------------------------------------------------------
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, avg, count, min, max, year
# from pyspark.sql.types import StructType, StructField, StringType, FloatType
# from kafka import KafkaConsumer
# import json
# from elasticsearch import Elasticsearch, exceptions
# import logging
# import time

# # Set up logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("HDFSReader") \
#     .getOrCreate()

# # Initialize Elasticsearch
# es_host = 'http://btl-bigdata-elasticsearch-1:9200'
# es = Elasticsearch([es_host])

# # Check Elasticsearch connection
# def check_es_connection():
#     try:
#         if es.ping():
#             logger.info("Successfully connected to Elasticsearch")
#         else:
#             raise ValueError("Elasticsearch connection failed")
#     except exceptions.ConnectionError as e:
#         logger.error("Elasticsearch connection error: {}".format(e))
#         raise e

# # Define schema for real estate data
# schema = StructType([
#     StructField("link", StringType(), True),
#     StructField("title", StringType(), True),
#     StructField("estate_type", StringType(), True),
#     StructField("province", StringType(), True),
#     StructField("district", StringType(), True),
#     StructField("ward", StringType(), True),
#     StructField("price", StringType(), True),  # Change to StringType
#     StructField("square", StringType(), True),  # Change to StringType
#     StructField("post_date", StringType(), True),
#     StructField("describe", StringType(), True)
# ])

# # Function to process and save data to Elasticsearch
# def process_and_save_to_es(df, epoch_id):
#     logger.info("Processing batch...")

#     # Convert columns to appropriate data types with error handling
#     df = df.withColumn("price", col("price").cast("float")).filter(col("price").isNotNull()) \
#            .withColumn("square", col("square").cast("float")).filter(col("square").isNotNull())

#     # Calculate average price and square footage by province
#     province_stats = df.groupBy("province").agg(
#         avg("price").alias("avg_price"),
#         count("price").alias("count_listings"),
#         min("price").alias("min_price"),
#         max("price").alias("max_price"),
#         avg("square").alias("avg_square")
#     )
    
#     # Calculate average price and square footage by estate type
#     estate_type_stats = df.groupBy("estate_type").agg(
#         avg("price").alias("avg_price"),
#         avg("square").alias("avg_square"),
#         count("estate_type").alias("count_estate_type")
#     )

#     # Estate type ratio
#     estate_type_ratio = df.groupBy("estate_type").agg(
#         count("estate_type").alias("count_estate_type")
#     )
#     total_estates = df.count()
#     estate_type_ratio = estate_type_ratio.withColumn("ratio", col("count_estate_type") / total_estates)

#     # Price trend by year
#     df_with_year = df.withColumn("year", year("post_date"))
#     price_trend = df_with_year.groupBy("year").agg(
#         avg("price").alias("avg_price"),
#         min("price").alias("min_price"),
#         max("price").alias("max_price")
#     )
    
#     # Real estate density by district
#     district_density = df.groupBy("district").agg(
#         count("district").alias("count_listings")
#     )

#     # Convert DataFrame to dictionary for indexing into Elasticsearch
#     province_stats_dict = province_stats.toJSON().collect()
#     estate_type_stats_dict = estate_type_stats.toJSON().collect()
#     estate_type_ratio_dict = estate_type_ratio.toJSON().collect()
#     price_trend_dict = price_trend.toJSON().collect()
#     district_density_dict = district_density.toJSON().collect()

#     # Save results to Elasticsearch
#     for record in province_stats_dict:
#         province_data = json.loads(record)
#         province_name = province_data["province"]
#         es.index(index="real_estate_analysis_province_stats", id=province_name, body={"doc": province_data, "doc_as_upsert": True})
    
#     for record in estate_type_stats_dict:
#         estate_type_data = json.loads(record)
#         estate_type_name = estate_type_data["estate_type"]
#         es.index(index="real_estate_analysis_estate_type_stats", id=estate_type_name, body={"doc": estate_type_data, "doc_as_upsert": True})

#     for record in estate_type_ratio_dict:
#         estate_type_ratio_data = json.loads(record)
#         estate_type_name = estate_type_ratio_data["estate_type"]
#         es.index(index="real_estate_analysis_estate_type_ratio", id=estate_type_name, body={"doc": estate_type_ratio_data, "doc_as_upsert": True})

#     for record in district_density_dict:
#         district_density_data = json.loads(record)
#         district_name = district_density_data["district"]
#         es.index(index="real_estate_analysis_district_density", id=district_name, body={"doc": district_density_data, "doc_as_upsert": True})

#     records = df.toJSON().collect()
#     for record in records:
#         es.index(index="real_estate_analysis", body=json.loads(record))
    
#     logger.info("Batch processed and saved to Elasticsearch.")

# # Create Kafka Consumer
# consumer = KafkaConsumer(
#     'real_estate_data',
#     bootstrap_servers='btl-bigdata-kafka-1:9092',
#     value_deserializer=lambda m: json.loads(m.decode('utf-8'))
# )

# # Receive data from Kafka and convert to Spark DataFrame
# data = []
# for message in consumer:
#     record = message.value
#     data.append(record)
#     logger.info("Received record: {}".format(record))
    
#     # When data reaches a certain amount, perform analysis and save to Elasticsearch
#     if len(data) >= 5:  # The number can be adjusted based on requirements
#         df = spark.createDataFrame(data, schema=schema)
        
#         # Apply schema and convert to DataFrame
#         df = df.withColumn("price", col("price").cast("float")).filter(col("price").isNotNull()) \
#                .withColumn("square", col("square").cast("float")).filter(col("square").isNotNull()) \
#                .withColumn("post_date", col("post_date").cast(StringType()))
        
#         # Check Elasticsearch connection
#         try:
#             check_es_connection()
#             # Call function to process and save data
#             process_and_save_to_es(df, None)
#         except Exception as e:
#             logger.error("Error while processing data: {}".format(e))
        
#         # Clear old data after processing
#         data = []  # Using assignment instead of clear method for compatibility

