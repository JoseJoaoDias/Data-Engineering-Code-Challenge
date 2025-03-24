"""
Main sript uses the functions from another scrips to perform the tasks.

"""
# Import libraries
from pyspark.sql import SparkSession
import os
import logging
from conf import settings
from challenge_tasks.data_preparation import read_csv_into_pyspark_dataframe

# import warnings
# from conf import settings
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,DateType
# from pyspark.sql.functions import col,isnan, when, count,date_format,to_date, coalesce
# from pyspark.sql import functions as F

# Setup logging configuration
# Path to logs file
log_dir = os.path.abspath("logs")  


if not os.path.exists(log_dir):
    os.makedirs(log_dir)  # Creates directory if does not exists

log_file = os.path.join(log_dir, "data_main.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO, # logging level can be adjusted if necessary (DEBUG, INFO, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s", # logging message format
)

logger=logging.getLogger(__name__)

# Create Spark Session
spark = SparkSession.builder \
    .appName("Data Challeng App") \
    .config("spark.hadoop.hadoop.native.lib", "false") \
    .master("local[*]") \
    .getOrCreate()

## Part 1 - Tasks
# Task 1 - Loaded 3 datasets
df_sales=read_csv_into_pyspark_dataframe(file_path=settings.SALES_FILE)
df_products=read_csv_into_pyspark_dataframe(file_path=settings.PRODUCTS_FILE)
df_stores=read_csv_into_pyspark_dataframe(file_path=settings.STORES_FILE)
