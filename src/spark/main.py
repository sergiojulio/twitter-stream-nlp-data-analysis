from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window
from pyspark.sql.types import *
import pyspark.sql.functions as F

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0 pyspark-shell'

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

streamdf = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "trump") \
  .load() 

streamdf.printSchema()

schema = StructType([
    StructField("time", TimestampType()),
    StructField("text", StringType())
])

streamdf = streamdf.selectExpr("CAST(value AS STRING)") \
          .select(F.from_json("value", schema=schema).alias("data")) \
          .select("data.*") 
          
# windowed_df = streamdf.groupBy(window("time", "10 seconds")).show()     

streamdf.writeStream  \
      .format("console")  \
      .outputMode("append")  \
      .option("checkpointLocation", "/home/sergio/dev/docker/twitter-stream-nlp-data-analysis/src/kafka/") \
      .trigger(continuous='5 seconds') \
      .start() \
      .awaitTermination()  

