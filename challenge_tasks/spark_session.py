# Create Spark Session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Data EXport App").getOrCreate()
