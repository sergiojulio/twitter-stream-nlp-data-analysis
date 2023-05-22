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



kafka_producer = KafkaProducer(
                    bootstrap_servers='localhost:9092', 
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                )

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

        # lang filter here
        if data['data']['lang'] == 'en':
            try:
                topic_name = "trump"
                now = datetime.datetime.utcnow()
                now = int(now.timestamp())
                # key_bytes = bytes(now)
                text = data['data']['text']
                # value_bytes = bytes(','.join([str(now), text]), encoding='utf-8')
                #kafka_producer.send(topic_name, key=now, value=value_bytes)
                kafka_producer.send(topic_name, value={'time': now, 'text': text})
                ##kafka_producer.send(topic_name, value=value_bytes)
                # kafka_producer.flush()
                print('Message published successfully.')
            except Exception as ex:
                print(str(ex))
            
            #print(','.join([str(now), text]))
            print(data)
            print("-"*50)

        return True    
    
    def on_error(self, status):
        print(status)


bearer_token = os.getenv('BEARER_TOKEN')

printer = TweetPrinterV2(bearer_token)

#printer.delete_rules([1659153209892429824])

#printer.add_rules(StreamRule(value="Madrid"))

print(printer.get_rules())

printer.filter(tweet_fields="created_at,geo,id,lang,text")

"""
client.filter(
expansions="attachments.poll_ids,attachments.media_keys,author_id,geo.place_id,in_reply_to_user_id,referenced_tweets.id,entities.mentions.username,referenced_tweets.id.author_id",
tweet_fields="attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld,edit_history_tweet_ids,edit_controls",
poll_fields="duration_minutes,end_datetime,id,options,voting_status",
place_fields="contained_within,country,country_code,full_name,geo,id,name,place_type",
user_fields="created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld",
media_fields="duration_ms,height,media_key,preview_image_url,public_metrics,type,url,width"
)

--------------------------------------------------
Message published successfully.
{'data': {'created_at': '2023-04-13T12:53:59.000Z', 'edit_history_tweet_ids': ['1646496954824953856'], 
'geo': {}, 'id': '1646496954824953856', 'lang': 'en', 
'text': 'RT @TheoHernandez: Good team effort ✅ This is @acmilan!!! 🔴⚫ #UCL #SempreMilan https://t.co/to30yHc7X8'}, 
'matching_rules': [{'id': '1645922914762407937', 'tag': ''}]}
--------------------------------------------------
"""


