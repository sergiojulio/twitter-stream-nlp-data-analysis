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


if __name__ == '__main__':
  main()