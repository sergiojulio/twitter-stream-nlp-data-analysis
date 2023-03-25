import tweepy
from tweepy import StreamingClient, StreamRule
import os
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
from pathlib import Path
import json
import datetime
import struct

dotenv_path = Path('.venv')
load_dotenv(dotenv_path=dotenv_path)



kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092')

class TweetPrinterV2(tweepy.StreamingClient):
    """
    def on_tweet(self, tweet):
        #kafka_producer.send("trump", tweet.text).get(timeout=1000)
        print(f"{tweet.id} {tweet.created_at} ({tweet.author_id}): {tweet.text}")
        print("-"*50)
    """


    def on_data(self, data):
        # kafka_producer.send("trump", data['text']).get(timeout=1000)

        data = json.loads(data)
        
        try:
            topic_name = "trump"
            now = datetime.datetime.utcnow()
            now = int(now.timestamp())
            key_bytes = bytes(now)
            text = data['data']['text']
            value_bytes = bytes(text, encoding='utf-8')
            kafka_producer.send('trump', value=value_bytes)
            # kafka_producer.send(topic_name, key=key_bytes, value=value_bytes)
            # kafka_producer.flush()
            print('Message published successfully.')
        except Exception as ex:
            print(str(ex))
        


        print(data['data']['text'])
        print("-"*50)
        return True    
    
    def on_error(self, status):
        print(status)


bearer_token = os.getenv('BEARER_TOKEN')

printer = TweetPrinterV2(bearer_token)
rule = StreamRule(value="#saime")
printer.add_rules(rule)
printer.filter()
