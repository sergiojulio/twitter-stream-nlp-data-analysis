# https://improveandrepeat.com/2022/04/python-friday-117-streaming-search-results-with-tweepy/
import tweepy
from tweepy import StreamingClient, StreamRule
import os


# bearer_token = os.getenv('BEARER_TOKEN')

class Twitterapi():

    def __init__(self, bearer_token):
        self.bearer_token = bearer_token
        

    @staticmethod
    def on_tweet(self, tweet):
        print(f"{tweet.id} {tweet.created_at} ({tweet.author_id}): {tweet.text}")
        print("-"*50)


    @staticmethod
    def stream(self, hashtag):
        printer = tweepy.StreamingClient(self.bearer_token) 
        rule = StreamRule(value=hashtag)
        printer.add_rules(rule)
        printer.filter()


    @staticmethod
    def test(hashtag):
        return hashtag

"""
1621632802184728576 None (None): Oferta laboral: Python Backend Developer - Buscamos un Python Backend Developer para una reconocida Institución... https://t.co/qdfd0vvZTi
"""