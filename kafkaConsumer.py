from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'crypto',
    bootstrap_servers=['hadoop-master:9092'])

for message in consumer:
    message = message.value
    print(message)
