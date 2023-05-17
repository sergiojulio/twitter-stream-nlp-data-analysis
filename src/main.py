from fastapi import FastAPI
from dotenv import load_dotenv
import os
from pathlib import Path
from twitter.twitterapi import Twitterapi


# dev
dotenv_path = Path('.venv')
load_dotenv(dotenv_path=dotenv_path)

app = FastAPI()

# arg hashtag - lang
@app.get("/")
async def root():
    # init twitter
    bearer_token = os.getenv('BEARER_TOKEN')
    twitter = Twitterapi(bearer_token)
    # twitter.stream("saime")

    # init kafka

    # init twitter 


    # init kafka
    return {"message": twitter.test("Hello World")}


@app.get("/token")
async def root():
    return {"token": os.getenv('BEARER_TOKEN')}