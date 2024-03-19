from pyspark.sql import SparkSession
from textblob import TextBlob

def init_spark():

  """
  conf = pyspark.SparkConf().setAppName('MyApp').setMaster('spark://spark-master:7077')
  sc = pyspark.SparkContext(conf=conf)
  """

  spark = SparkSession.builder.appName("HelloWorld").master('spark://spark:7077').getOrCreate()
  sc = spark.sparkContext
  return spark,sc

  """
  Without spark action APIs(collect/take/first/saveAsTextFile) nothing will be executed on executors. 
  Its not possible to distribute plain python code just by submitting to spark. 
  """

def main():
  spark,sc = init_spark()
  nums = sc.parallelize([1,2,3,4])
  print(nums.map(lambda x: x*x).collect())
  print("Holi")
  print(sc.getConf().getAll())

  """
   spark = SparkSession \
      .builder \
      .appName("Python Spark SQL basic example") \
      .config("spark.jars", "/code/src/spark/postgresql-42.6.2jar") \
      .getOrCreate()

  df = spark.read \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://postgres:5432/postgres_db") \
      .option("dbtable", "stream") \
      .option("user", "postgres") \
      .option("password", "postgres") \
      .option("driver", "org.postgresql.Driver") \
      .load()

  df.printSchema()
 
  """

  text = """
  The titular threat of The Blob has always struck me as the ultimate movie
  monster: an insatiably hungry, amoeba-like mass able to penetrate
  virtually any safeguard, capable of--as a doomed doctor chillingly
  describes it--"assimilating flesh on contact.
  Snide comparisons to gelatin be damned, it's a concept with the most
  devastating of potential consequences, not unlike the grey goo scenario
  proposed by technological theorists fearful of
  artificial intelligence run rampant.
  """

  blob = TextBlob(text)
  blob.tags  # [('The', 'DT'), ('titular', 'JJ'),
  #  ('threat', 'NN'), ('of', 'IN'), ...]

  blob.noun_phrases  # WordList(['titular threat', 'blob',
  #            'ultimate movie monster',
  #            'amoeba-like mass', ...])

  for sentence in blob.sentences:
      print(sentence.sentiment.polarity)


if __name__ == '__main__':
  main()