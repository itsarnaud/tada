from pyspark.sql import SparkSession

def create_spark_session(name):
  return SparkSession.builder.appName(name).getOrCreate()

def read_csv(spark, path, sep):
  df = spark.read.csv(path, header=True, inferSchema=True, sep=sep)
  return df
