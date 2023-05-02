from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window
from pyspark.sql.types import TimestampType, StringType, IntegerType
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
import time
import re

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0 pyspark-shell'


def write_to_mysql(df, epoch_id):
    df.show()
    print('hi')
    pass


def myFunction(num):
    return num


def clean_text(sentence):
    sentence = sentence.lower()
    sentence = re.sub("s+"," ", sentence)
    sentence = re.sub("W"," ", sentence)
    sentence = re.sub(r"httpS+", "", sentence)
    return sentence.strip()



if __name__ == "__main__":
        
    print("Stream Data Processing Starting ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
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

    udf_myFunction = udf(myFunction, IntegerType()) # if the function returns an int

    streamdf = streamdf.selectExpr("CAST(value AS STRING)") \
            .select(F.from_json("value", schema=schema).alias("data")) \
            .select("data.*") \
            .withColumn("newlyCalculatedColumnName", F.lit("npl"))

    # output

    csv_output = streamdf \
        .writeStream \
        .format("csv")\
        .option("format", "append")\
        .trigger(processingTime = "5 seconds")\
        .option("path", "/home/sergio/dev/docker/twitter-stream-nlp-data-analysis/src/kafka/csv")\
        .option("checkpointLocation", "/home/sergio/dev/docker/twitter-stream-nlp-data-analysis/src/kafka/checkpoint") \
        .outputMode("append") \
        .start()
    
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
        .foreachBatch(write_to_mysql) \
        .start()
        #.awaitTermination() 

    spark.streams.awaitAnyTermination()
    
    print("Stream Processing Successfully Completed ! ! !")








