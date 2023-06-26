from fastapi import FastAPI
from dotenv import load_dotenv
import os
from pathlib import Path
# import kafka
from kafka import KafkaConsumer, KafkaProducer
from src.twitter.twitterapi import Twitterapi
import json
import time,csv


# dev
dotenv_path = Path('.venv')
load_dotenv(dotenv_path=dotenv_path)

app = FastAPI()


# arg hashtag - lang
@app.get("/")
async def root():

    bearer_token = os.getenv('BEARER_TOKEN')

     # init kafka
    """
        producer = KafkaProducer(
                    bootstrap_servers='localhost:9092', 
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                )   
    """ 
    producer = KafkaProducer(
                bootstrap_servers='localhost:9092'
            )  
    

    # with open csv
    fname = "/home/sergio/dev/docker/twitter-stream-nlp-data-analysis/src/twitter/tweets.csv"
    divider_char = ','
    # open file
    with open(fname) as fp:  
        # read header (first line of the input file)
        line = fp.readline()
        header = line.split(divider_char)

        #loop other data rows 
        line = fp.readline()    
        while line:
            # start to prepare data row to send
            data_to_send = ""
            values = line.split(divider_char)
            len_header = len(header)
            for i in range(len_header):
                data_to_send += "\""+header[i].strip()+"\""+":"+"\""+values[i].strip()+"\""
                if i<len_header-1 :
                    data_to_send += ","
            data_to_send = "{"+data_to_send+"}"

            '''
            example of outputs is valid JSON row 
            {
                "AT":"0.148251748251748",
                "BE":"0.052603706790461",
                    ...
                "SE":"0.0826699344612236",
                "UK":"0.10951678628072099"
            }
            '''

            # send data via producer
            producer.send('trump', bytes(data_to_send, encoding='utf-8'))
            line = fp.readline()
            # А это так))) на всякий случай
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
    return {"message": "hi"}


@app.get("/token")
async def root():
    return {"token": os.getenv('BEARER_TOKEN')}