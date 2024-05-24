# from kafka import KafkaProducer
# import json
# import time

# producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# data = [
#     {"property_id": 1, "price": 300000, "location": "City A"},
#     {"property_id": 2, "price": 450000, "location": "City B"},
#     # Add more data as needed
# ]

# while True:
#     for record in data:
#         producer.send('real_estate_data', json.dumps(record).encode('utf-8'))  # Convert record to JSON string here
#         print(f'Sent: {record}')
#     time.sleep(5)

from kafka import KafkaProducer
import json
import time

# Thay đổi cổng kết nối Kafka thành 9093
producer = KafkaProducer(bootstrap_servers='localhost:9093', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

data = [
    {"property_id": 1, "price": 300000, "location": "City A"},
    {"property_id": 2, "price": 450000, "location": "City B"},
    # Add more data as needed
]

while True:
    for record in data:
        producer.send('real_estate_data', record)
        print(f'Sent: {record}')
    time.sleep(5)

