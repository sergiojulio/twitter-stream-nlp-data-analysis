# https://improveandrepeat.com/2022/04/python-friday-117-streaming-search-results-with-tweepy/
import tweepy
from tweepy import StreamingClient, StreamRule
import os
import json
import datetime


class Twitterapi():

    def __init__(self, bearer_token):
        self.bearer_token = bearer_token

    # add hash tag
        
    # remove hash tag
    """
    @staticmethod
    def on_tweet(self, tweet):
        print(f"{tweet.id} {tweet.created_at} ({tweet.author_id}): {tweet.text}")
        print("-"*50)
    """


    @staticmethod
    def stream(self, hashtag):
        printer = tweepy.StreamingClient(self.bearer_token) 
        rule = StreamRule(value=hashtag)
        printer.add_rules(rule)
        printer.filter()


    @staticmethod
    def test(hashtag):
        return hashtag
    

    @staticmethod
    def on_data(self, data):
        
        data = json.loads(data)
        # lang filter here
        if data['data']['lang'] == 'en':
            try:
                topic_name = "trump"
                now = datetime.datetime.utcnow()
                now = int(now.timestamp())
                text = data['data']['text']
                # if loaded
                kafka_producer.send(topic_name, value={'time': now, 'text': text})
                # 
            except Exception as ex:
                print(str(ex))
            
            print(data)
            print("-"*50)

        return True    
    

    @staticmethod
    def on_error(self, status):
        print(status)
