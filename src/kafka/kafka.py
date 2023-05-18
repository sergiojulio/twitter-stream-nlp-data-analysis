from kafka import KafkaConsumer, KafkaProducer
import json

kafka_producer = KafkaProducer(
                    bootstrap_servers='localhost:9092', 
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                )

