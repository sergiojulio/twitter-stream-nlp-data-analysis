from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window
from pyspark.sql.types import TimestampType, StringType, IntegerType
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
import time
import re
from textblob import TextBlob


def clean_tweet(tweet):
    stopwords = ["for", "on", "an", "a", "of", "and", "in", "the", "to", "from"]
    temp = tweet.lower()
    temp = re.sub("'", "", temp) 
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

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0 pyspark-shell'

def write_to_pgsql(df, epoch_id):
    df.show()
    print('hi')
    pass


def myFunction(string):

    blob = TextBlob(clean_tweet(string))

    p = 0
    c = 0
    i = 0
    for sentence in blob.sentences:
        print(sentence.sentiment.polarity)
        c = sentence.sentiment.polarity  + c
        i += 1

    p = c / i
    p = round(p,2)

    return p


def clean_text(sentence):
    sentence = sentence.lower()
    sentence = re.sub("s+"," ", sentence)
    sentence = re.sub("W"," ", sentence)
    sentence = re.sub(r"httpS+", "", sentence)
    return sentence.strip()



if __name__ == "__main__":

    # create database
        
    print("Stream Data Processing Starting ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))


    spark = SparkSession \
        .appName("StructuredNetworkWordCount") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    streamdf = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "trump") \
        .option("startingOffsets", "latest") \
        .load() 
    
    print("Printing Schema of orders_df: ")

    streamdf.printSchema()

    schema = StructType([
        StructField("time", TimestampType()),
        StructField("text", StringType())
    ])

    udf_myFunction = udf(myFunction, StringType()) # if the function returns an int

    streamdf = streamdf.selectExpr("CAST(value AS STRING)") \
            .select(F.from_json("value", schema=schema).alias("data")) \
            .select("data.*") \
            .withColumn("newlyCalculatedColumnName", F.lit("npl"))

    # output

    # csv_output = streamdf \
    #     .writeStream \
    #     .format("csv")\
    #     .option("format", "append")\
    #     .trigger(processingTime = "5 seconds")\
    #     .option("path", "/home/sergio/dev/docker/twitter-stream-nlp-data-analysis/src/kafka/csv")\
    #     .option("checkpointLocation", "/home/sergio/dev/docker/twitter-stream-nlp-data-analysis/src/kafka/checkpoint") \
    #     .outputMode("append") \
    #     .start()
    
    # spark.read.csv("oldLocation").coalesce(1).write.csv("newLocation")

    console_output = streamdf \
        .writeStream  \
        .trigger(processingTime='5 seconds') \
        .outputMode("update")  \
        .option("truncate", "false")\
        .format("console") \
        .start() 
        #.awaitTermination()

    db_output = streamdf \
        .writeStream  \
        .trigger(processingTime='5 seconds') \
        .outputMode("update")  \
        .foreachBatch(write_to_pgsql) \
        .start()
        #.awaitTermination() 

    spark.streams.awaitAnyTermination()
    
    print("Stream Processing Successfully Completed")

