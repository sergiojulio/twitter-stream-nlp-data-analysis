from fastapi import FastAPI
from dotenv import load_dotenv
import os
from pathlib import Path
# import kafka
from kafka import KafkaConsumer, KafkaProducer
from src.twitter.twitterapi import Twitterapi
import json


# dev
dotenv_path = Path('.venv')
load_dotenv(dotenv_path=dotenv_path)

app = FastAPI()


# arg hashtag - lang
@app.get("/")
async def root():

    bearer_token = os.getenv('BEARER_TOKEN')

     # init kafka
    kafka_producer = KafkaProducer(
                    bootstrap_servers='localhost:9092', 
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                )   
    
    # init twitter 
    printer = Twitterapi(bearer_token, kafka_producer)

    # init streaming 
    printer.stream("saime")
    
    # printer.filter(tweet_fields="created_at,geo,id,lang,text")


    # printer = TweetPrinterV2(bearer_token)
    # print(printer.get_rules())
    # printer.filter(tweet_fields="created_at,geo,id,lang,text")

    # init kafka
    return {"message": "hi"}


@app.get("/token")
async def root():
    return {"token": os.getenv('BEARER_TOKEN')}