from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='hadoop-master:9092')
producer.send('crypto', b'Hello, World!')
producer.close()