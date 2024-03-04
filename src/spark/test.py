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


  jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql:postgres:5432") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .load()
  # Specifying create table column data types on write
  jdbcDF.write \
    .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)") \
    .jdbc("jdbc:postgresql:postgres:5432", "schema.postgres",
          properties={"user": "postgres", "password": "postgres"})


if __name__ == '__main__':
  main()