# Create Spark Session
from pyspark.sql import SparkSession, DataFrame

spark = (
    SparkSession.builder.appName("Data EXport App")
    .config("spark.hadoop.hadoop.native.lib", "false")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.maxResultSize", "4g")
    .master("local[*]")
    .getOrCreate()
)
