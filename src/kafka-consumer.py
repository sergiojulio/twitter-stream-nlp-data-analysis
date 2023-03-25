from kafka import KafkaConsumer
consumer = KafkaConsumer('trump')
for message in consumer:
    print (message)