# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, avg, count, concat, lit
# from pyspark.sql.types import StructType, StructField, StringType
# from kafka import KafkaConsumer
# import json
# from elasticsearch import Elasticsearch, helpers, exceptions
# import logging
# import re
# import sys

# # Set up logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


# reload(sys)
# sys.setdefaultencoding('utf-8')

# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("HDFSReader") \
#     .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
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
#     StructField("price", StringType(), True),
#     StructField("square", StringType(), True),
#     StructField("post_date", StringType(), True),
#     StructField("describe", StringType(), True),
#     StructField("price/square", StringType(), True),
# ])

# # Process and save data to Elasticsearch
# def process_and_save_to_es(df, epoch_id):
#     logger.info("Processing batch...")

#     # Convert columns to appropriate data types with error handling
#     df = df.withColumn("price", col("price").cast("float")).filter(col("price").isNotNull()) \
#            .withColumn("square", col("square").cast("float")).filter(col("square").isNotNull()) \
#            .withColumn("price/square", col("price/square").cast("float")).filter(col("price/square").isNotNull())

#     # Extract year and month from post_date string
#     df = df.withColumn("year", df["post_date"].substr(7, 4)) \
#            .withColumn("month", df["post_date"].substr(4, 2)) \
#            .withColumn("month_year", df["post_date"].substr(4, 7))
    

#     df = df.withColumn("dis_month_year", concat(df["district"],lit("-"), df["month_year"]))\
#            .withColumn("dis_year", concat(df["district"],lit("-"), df["year"]))
     

#     df.show()
#     # Calculate average price and square footage by district per month/year
#     district_stats_monthly = df.groupBy("district", "month_year","dis_month_year").agg(
#         avg("square").alias("avg_square"),
#         avg("price/square").alias("avg_price_per_square"),
#         count("square").alias("count_house")
#     )

#     district_stats_yearly = df.groupBy("district", "year").agg(
#         avg("square").alias("avg_square"),
#         avg("price/square").alias("avg_price_per_square")
#     )

#     # Convert DataFrame to dictionary for indexing into Elasticsearch
#     district_stats_monthly_dict = district_stats_monthly.toJSON().collect()
#     district_stats_yearly_dict = district_stats_yearly.toJSON().collect()

#     # Save results to Elasticsearch using bulk API
#     actions = []

#     for record in district_stats_monthly_dict:
#         district_monthly_data = json.loads(record)
#         district_name = district_monthly_data["district"]
#         month_year = district_monthly_data["month_year"]
#         dis_month_year = district_monthly_data["dis_month_year"]

#         actions.append({
#             "_index": "real_estate_analysis_district_stats_monthly",
#             "_id": dis_month_year,
#             "_source": {"doc": district_monthly_data, "doc_as_upsert": True}
#         })

#     for record in district_stats_yearly_dict:
#         district_yearly_data = json.loads(record)
#         district_name = district_yearly_data["district"]
#         dis_year = district_yearly_data["dis_year"]
#         actions.append({
#             "_index": "real_estate_analysis_district_stats_yearly",
#             "_id": dis_year,
#             "_source": {"doc": district_yearly_data, "doc_as_upsert": True}
#         })

#     helpers.bulk(es, actions)
#     logger.info("Batch processed and saved to Elasticsearch.")

# # Create Kafka Consumer
# consumer = KafkaConsumer(
#     'real_estate_data',
#     bootstrap_servers='btl-bigdata-kafka-1:9092',
#     value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#     # auto_offset_reset='earliest'  # Ensures consumer reads from the start if no offset is present
# )

# # Receive data from Kafka and convert to Spark DataFrame
# data = []
# for message in consumer:
#     record = message.value
#     data.append(record)
#     logger.info("Received record: {}".format(record))
    
#     # When data reaches a certain amount, perform analysis and save to Elasticsearch
#     if len(data) >= 10:  # Increase the number of records to process each time
#         df = spark.createDataFrame(data, schema=schema)
        
#         # Check Elasticsearch connection
#         try:
#             check_es_connection()
#             # Call function to process and save data
#             process_and_save_to_es(df, None)
#         except Exception as e:
#             logger.error("Error while processing data: {}".format(e))
        
#         # Clear the data list for the next batch
#         # data.clear()







# ----------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, min, max, concat, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch, helpers, exceptions
import logging
import re
import sys

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Doc duoc nhung ky tu dac biet
reload(sys)
sys.setdefaultencoding('utf-8')
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
    StructField("describe", StringType(), True),
    StructField("price/square", StringType(), True), # Change to StringType
])


# Process and save data to Elasticsearch
def process_and_save_to_es(df, epoch_id):
    logger.info("Processing batch...")

    # Convert columns to appropriate data types with error handling
    df = df.withColumn("price", col("price").cast("float")).filter(col("price").isNotNull()) \
           .withColumn("square", col("square").cast("float")).filter(col("square").isNotNull())\
           .withColumn("price/square", col("price/square").cast("float")).filter(col("price/square").isNotNull()) 
    
        # Extract year and month from post_date string
    df = df.withColumn("year", df["post_date"].substr(7, 4)) \
           .withColumn("month", df["post_date"].substr(4, 2)) \
           .withColumn("month_year", df["post_date"].substr(4, 7))
    

    df = df.withColumn("dis_month_year", concat(df["district"],lit("-"), df["month_year"]))\
           .withColumn("dis_year", concat(df["district"],lit("-"), df["year"]))

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
        avg("price/square").alias("avg_price_square"),
        avg("square").alias("avg_square"),
        count("estate_type").alias("count_estate_type")
    )

    # Estate type ratio
    estate_type_ratio = df.groupBy("estate_type").agg(
        count("estate_type").alias("count_estate_type")
    )
    total_estates = df.count()
    estate_type_ratio = estate_type_ratio.withColumn("ratio", col("count_estate_type") / total_estates)


    # Real estate density by district
    district_density = df.groupBy("district").agg(
        count("district").alias("count_listings"),
        avg("price/square").alias("avg_price_per_square"),
        avg("square").alias("avg_square"),
        min("price/square").alias("min_price_square"),
        max("price/square").alias("max_price_square"),
    )
    # Price trend by year
    price_trend = df.groupBy("month","year","month_year").agg(
        avg("price/square").alias("avg_price_per_square"),
        avg("square").alias("avg_square"),
        min("price/square").alias("min_price_square"),
        max("price/square").alias("max_price_square"),
        count("link").alias("count_house")
    )
    

# Timer
    district_stats_monthly = df.groupBy("district","month","year","month_year","dis_month_year").agg(
        avg("square").alias("avg_square"),
        avg("price/square").alias("avg_price_per_square"),
        min("price/square").alias("min_price_square"),
        max("price/square").alias("max_price_square"),
        count("link").alias("count_house")
    )

    district_stats_yearly = df.groupBy("district", "year","dis_year").agg(
        avg("square").alias("avg_square"),
        avg("price/square").alias("avg_price_per_square"),
        count("link").alias("count_house")
    )


    # Convert DataFrame to dictionary for indexing into Elasticsearch
    records_dict = df.toJSON().collect() 
    province_stats_dict = province_stats.toJSON().collect()
    estate_type_stats_dict = estate_type_stats.toJSON().collect()
    estate_type_ratio_dict = estate_type_ratio.toJSON().collect()
    district_density_dict = district_density.toJSON().collect()
    price_trend_dict = price_trend.toJSON().collect()
    district_stats_monthly_dict = district_stats_monthly.toJSON().collect()
    district_stats_yearly_dict = district_stats_yearly.toJSON().collect()

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
    
    for record in price_trend_dict:
        price_trend_data = json.loads(record)
        month_year = price_trend_data["month_year"]
        actions.append({
            "_index": "real_estate_analysis_monthly_year",
            "_id": month_year,
            "_source": {"doc": price_trend_data, "doc_as_upsert": True}
        })


    for record in district_stats_monthly_dict:
        district_monthly_data = json.loads(record)
        district_name = district_monthly_data["district"]
        month_year = district_monthly_data["month_year"]
        dis_month_year = district_monthly_data["dis_month_year"]

        actions.append({
            "_index": "real_estate_analysis_district_stats_monthly",
            "_id": dis_month_year,
            "_source": {"doc": district_monthly_data, "doc_as_upsert": True}
        })

    for record in district_stats_yearly_dict:
        district_yearly_data = json.loads(record)
        district_name = district_yearly_data["district"]
        dis_year = district_yearly_data["dis_year"]
        actions.append({
            "_index": "real_estate_analysis_district_stats_yearly",
            "_id": dis_year,
            "_source": {"doc": district_yearly_data, "doc_as_upsert": True}
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
    'real_estate_data_',
    bootstrap_servers='btl-bigdata-kafka-1:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    # auto_offset_reset='earliest'  # Ensures consumer reads from the start if no offset is present
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
        # df = df.withColumn("price", col("price").cast("float")).filter(col("price").isNotNull()) \
        #        .withColumn("square", col("square").cast("float")).filter(col("square").isNotNull()) \
        #        .withColumn("price/square", col("price/square").cast("float")).filter(col("price/square").isNotNull()) \
        #        .withColumn("post_date", to_date(col("post_date"), "dd/MM/yyyy"))
            #    .withColumn("post_date", col("post_date").cast(StringType()))

        
        # Check Elasticsearch connection
        try:
            check_es_connection()
            # Call function to process and save data
            process_and_save_to_es(df, None)
        except Exception as e:
            logger.error("Error while processing data: {}".format(e))
        
        # # Clear old data after processing
        # data = []  # Using assignment instead of clear method for compatibility



