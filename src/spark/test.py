from pyspark.sql import SparkSession
import nltk

def init_spark():
  spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
  sc = spark.sparkContext
  return spark,sc

def main():
  spark,sc = init_spark()
  nums = sc.parallelize([1,2,3,4])
  print(nums.map(lambda x: x*x).collect())
  print("Holi")
  print(sc.getConf().getAll())


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


if __name__ == '__main__':
  main()