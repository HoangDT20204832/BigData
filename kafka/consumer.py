# from kafka import KafkaConsumer
# import json
# from elasticsearch import Elasticsearch

# consumer = KafkaConsumer(
#     'real_estate_data',
#     bootstrap_servers='localhost:9092',
#     value_deserializer=lambda m: json.loads(m.decode('utf-8'))
# )

# es = Elasticsearch(['http://localhost:9200'])

# for message in consumer:
#     record = message.value
#     es.index(index='real_estate', doc_type='_doc', body=record)
#     print(f'Indexed: {record}')

# from kafka import KafkaConsumer
# import json
# from elasticsearch import Elasticsearch

# consumer = KafkaConsumer(
#     'real_estate_data',
#     bootstrap_servers='localhost:9092',
#     value_deserializer=lambda m: json.loads(m.decode('utf-8'))
# )

# es = Elasticsearch(['http://localhost:9200'])

# for message in consumer:
#     record = message.value
#     es.index(index='real_estate', doc_type='_doc', body=record)
#     print(f'Indexed: {record}')
from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch

# Thay đổi cổng kết nối Kafka thành 9093
consumer = KafkaConsumer(
    'real_estate_data',
    bootstrap_servers='localhost:9093',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

es = Elasticsearch(['http://localhost:9200'])

for message in consumer:
    record = message.value
    es.index(index='real_estate', doc_type='_doc', body=record)
    print(f'Indexed: {record}')

