import tweepy
from tweepy import StreamingClient, StreamRule
import os
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
from pathlib import Path
import json
import datetime
import struct
import msgpack

dotenv_path = Path('/home/sergio/dev/docker/twitter-stream-nlp-data-analysis/.venv')
load_dotenv(dotenv_path=dotenv_path)



kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=msgpack.dumps)

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
            # key_bytes = bytes(now)
            text = data['data']['text']
            value_bytes = bytes(','.join([str(now), text]), encoding='utf-8')
            #kafka_producer.send(topic_name, key=now, value=value_bytes)
            # kafka_producer.send(topic_name, {'time': now, 'text': text})
            kafka_producer.send(topic_name, value=value_bytes)
            # kafka_producer.flush()
            print('Message published successfully.')
        except Exception as ex:
            print(str(ex))
        


        print(','.join([str(now), text]))
        print("-"*50)
        return True    
    
    def on_error(self, status):
        print(status)


bearer_token = os.getenv('BEARER_TOKEN')

printer = TweetPrinterV2(bearer_token)
# rule = StreamRule(value="Bayern")

# printer.delete_rules([1645923451603918853])



"""
{'edit_history_tweet_ids': ['1645940261066018817'], 'id': '1645940261066018817', 'text': '🏆 UEFA Champions League • Quarter-final • 1st Leg⚽️\n\n🆚 AC Milan v Napoli\n⏰ Wed, Apr 12, 21:00 🇮🇹\n\n▶️ Live Stream 🔴\n\n#UCL #SerieA #ChampionsLeague #MILNAP #Italy #Napoli #UEFA #ACMilan #MilanNapoli https://t.co/wiolyWypgk'}
"""



#printer.add_rules(rule)

#print(printer.get_rules())

printer.filter()
