from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window
from pyspark.sql.types import *

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0 pyspark-shell'


def parse_data_from_kafka_message(sdf, schema):
    from pyspark.sql.functions import split
    assert sdf.isStreaming == True, "DataFrame doesn't receive treaming data"
    print(sdf['value'])
    col = split(sdf['value'], ',') #split attributes to nested array in one Column
    #now expand col to multiple top-level columns
    for idx, field in enumerate(schema): 
        sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return sdf.select([field.name for field in schema])

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
  .load() \
  .selectExpr("CAST(value AS STRING)")



"""
root
 |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true)

"""

schema = StructType([ \
    StructField("time", LongType()), StructField("time", TimestampType()), \
    StructField("text", StringType()), StructField("text", StringType())  ])

dsraw = parse_data_from_kafka_message(dsraw, schema)

ds = dsraw.withWatermark("time", "10 seconds") \
          .groupBy(window(dsraw.time, "10 seconds")) 

dsraw.printSchema()

"""

kafka_df \
    .withWatermark("timestamp", "5 seconds") \
    .groupBy(window(kafka_df.timestamp, "5 seconds", "1 second"),kafka_df.value) 

    Silver = (Bronze 
    .withWatermark("TimeStamp", "1 minute")
    .groupBy(['sensor_id', F.window('TimeStamp', '1 minute')])
)




rawQuery = ds \
        .writeStream \
        .queryName("qraw")\
        .format("memory")\
        .start()

raw = spark.sql("select * from qraw")
raw.show()
rawQuery.awaitTermination()
"""
dsraw.writeStream  \
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