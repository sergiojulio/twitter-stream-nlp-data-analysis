from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0 pyspark-shell'

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# Subscribe to 1 topic
dsraw = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "trump") \
  .option("startingOffsets", "earliest") \
  .load()

dsraw.printSchema()

ds = dsraw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")




kafka_df \
    .withWatermark("timestamp", "5 seconds") \
    .groupBy(window(kafka_df.timestamp, "5 seconds", "1 second"),kafka_df.value) 


"""
rawQuery = ds \
        .writeStream \
        .queryName("qraw")\
        .format("memory")\
        .start()

raw = spark.sql("select * from qraw")
raw.show()
rawQuery.awaitTermination()
"""
ds.writeStream  \
      .format("console")  \
      .outputMode("append")  \
      .start()  \
      .awaitTermination()  

"""
-------------------------------------------
Batch: 20
-------------------------------------------
+----+--------------------+
| key|               value|
+----+--------------------+
|null|RT @AlfredoARamos...|
|null|RT @DonGochoK: Mu...|
+----+--------------------+

-------------------------------------------
Batch: 21
-------------------------------------------
+----+--------------------+
| key|               value|
+----+--------------------+
|null|@monitoreamos Esa...|
+----+--------------------+

"""