# Create Spark Session
from pyspark.sql import SparkSession


spark = (
    SparkSession.builder.appName("Data Export App")
    .config("spark.hadoop.hadoop.native.lib", "false")
    .master("local[*]")
    .getOrCreate()
)
