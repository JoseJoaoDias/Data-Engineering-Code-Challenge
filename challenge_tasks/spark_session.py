# Create Spark Session
from pyspark.sql import SparkSession


spark = (
    SparkSession.builder.appName("Data Export App")
    .config("spark.hadoop.hadoop.native.lib", "false")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.maxResultSize", "4g")
    .master("local[*]")
    .getOrCreate()
)
