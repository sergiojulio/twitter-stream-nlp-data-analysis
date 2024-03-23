from fastapi import FastAPI
from dotenv import load_dotenv
import os
from pathlib import Path
# import kafka
from kafka import KafkaConsumer, KafkaProducer
from src.app.twitter.twitterapi import Twitterapi
import json
import time,csv


# dev
dotenv_path = Path('.venv')
load_dotenv(dotenv_path=dotenv_path)

app = FastAPI()


# arg hashtag - lang
@app.get("/")
async def root():


     # init kafka
    """
    bearer_token = os.getenv('BEARER_TOKEN')
    producer = KafkaProducer(
                bootstrap_servers='localhost:9092', 
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            )   
    """ 
    
    producer = KafkaProducer(bootstrap_servers='kafka:9093')  
    

    # with open csv
    csvfile = open("/code/src/app/twitter/tweets.csv","r")
    # csvfile = open("/home/sergio/dev/docker/twitter-stream-nlp-data-analysis/src/app/twitter/tweets.csv","r")

    reader = csv.DictReader(csvfile)
    for row in reader:

        data_to_send = json.dumps(row) 

        """
        {"tweet_id": "569281033365037056", 
        "airline_sentiment": "negative", 
        "airline_sentiment_confidence": "1.0", 
        "negativereason": "Lost Luggage", 
        "negativereason_confidence": "0.6882", 
        "airline": "United", 
        "airline_sentiment_gold": "", 
        "name": "hannahcbeck", 
        "negativereason_gold": "", 
        "retweet_count": "1", 
        "text": "@united No customer service rep could confirm where my bag was &amp; each gave me different info. Ruined 2 events I had today. #disappointed", 
        "tweet_coord": "[35.00096266, -80.87374938]", 
        "tweet_created": "2015-02-21 15:42:29 -0800", 
        "tweet_location": "Hoboken, NJ", 
        "user_timezone": "Eastern Time (US & Canada)"}
        """

        #print(data_to_send)

        # send data via producer
        producer.send('trump', bytes(data_to_send, encoding='utf-8'))
        # 
        time.sleep(1)





    producer.close()
    # init twitter 
    # printer = Twitterapi(bearer_token)
    # 
    # init streaming 
    # printer.stream("saime", kafka_producer)
    # 
    # printer.filter(tweet_fields="created_at,geo,id,lang,text")
    # printer = TweetPrinterV2(bearer_token)
    # print(printer.get_rules())
    # printer.filter(tweet_fields="created_at,geo,id,lang,text")
    # init kafka
    return {"message": "finished"}


@app.get("/token")
async def root():
    return {"token": os.getenv('BEARER_TOKEN')}


@app.get("/test")
async def root():
    producer = KafkaProducer(bootstrap_servers='kafka:9093')  # kafka debe venir del .env
    producer.send('trump', bytes('hola', encoding='utf-8'))
