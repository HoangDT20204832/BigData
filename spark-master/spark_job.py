from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder.appName('RealEstateAnalysis').getOrCreate()
df = spark.read.json('hdfs://hadoop-namenode:9000/path/to/your/real_estate_data.json')

avg_price = df.groupBy('location').agg(avg('price').alias('avg_price'))
avg_price.show()

avg_price.write.format('org.elasticsearch.spark.sql').option('es.resource', 'real_estate/analysis').save()

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import avg, col
# from pyspark.ml.feature import VectorAssembler
# from pyspark.ml.regression import LinearRegression

# spark = SparkSession.builder.appName('RealEstateAnalysis').getOrCreate()
# df = spark.read.json('hdfs://hadoop-namenode:9000/path/to/your/real_estate_data.json')

# # Tính giá trung bình theo khu vực
# avg_price = df.groupBy('location').agg(avg('price').alias('avg_price'))
# avg_price.show()

# # Huấn luyện mô hình dự báo giá bất động sản
# assembler = VectorAssembler(inputCols=['feature1', 'feature2', 'feature3'], outputCol='features')
# data = assembler.transform(df)
# data = data.select('features', col('price').alias('label'))

# # Chia dữ liệu thành tập huấn luyện và tập kiểm tra
# train_data, test_data = data.randomSplit([0.8, 0.2])

# # Huấn luyện mô hình hồi quy tuyến tính
# lr = LinearRegression()
# model = lr.fit(train_data)

# # Đánh giá mô hình
# test_results = model.evaluate(test_data)
# print(f'RMSE: {test_results.rootMeanSquaredError}')
# print(f'R2: {test_results.r2}')

# # Lưu kết quả vào Elasticsearch
# avg_price.write.format('org.elasticsearch.spark.sql').option('es.resource', 'real_estate/analysis').save()
