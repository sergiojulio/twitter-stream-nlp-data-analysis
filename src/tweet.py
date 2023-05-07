 
import numpy as np
import re
from textblob import TextBlob

def clean_tweet(tweet):
    stopwords = ["for", "on", "an", "a", "of", "and", "in", "the", "to", "from"]

    temp = tweet.lower()
    temp = re.sub("'", "", temp) # to avoid removing contractions in english
    temp = re.sub("@[A-Za-z0-9_]+","", temp)
    temp = re.sub("#[A-Za-z0-9_]+","", temp)
    temp = re.sub(r'http\S+', '', temp)
    temp = re.sub('[()!?]', ' ', temp)
    temp = re.sub('\[.*?\]',' ', temp)
    temp = re.sub("[^a-z0-9]"," ", temp)
    temp = temp.split()
    temp = [w for w in temp if not w in stopwords]
    temp = " ".join(word for word in temp)
    return temp



text = '''
today es a beautiful day. I am full of love. I want make you happy
'''

blob = TextBlob(clean_tweet(text))
blob.tags           # [('The', 'DT'), ('titular', 'JJ'),
                    #  ('threat', 'NN'), ('of', 'IN'), ...]

blob.noun_phrases   # WordList(['titular threat', 'blob',
                    #            'ultimate movie monster',
                    #            'amoeba-like mass', ...])

c = 0
i = 0
for sentence in blob.sentences:
    print(sentence.sentiment.polarity)
    c = sentence.sentiment.polarity  + c
    i += 1
# 0.060
# -0.341
p = c / i
p = round(p,2)

print(p)

#print(blob.detect_language())

