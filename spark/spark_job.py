from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder.appName('RealEstateAnalysis').getOrCreate()
df = spark.read.json('hdfs://hadoop-namenode:9000/path/to/your/real_estate_data.json')

avg_price = df.groupBy('location').agg(avg('price').alias('avg_price'))
avg_price.show()

avg_price.write.format('org.elasticsearch.spark.sql').option('es.resource', 'real_estate/analysis').save()
