"""
Main sript uses the functions from another scrips to perform the tasks.

"""
# Import libraries
from pyspark.sql import SparkSession
import os
import logging
from conf import settings
from challenge_tasks.data_preparation import read_csv_into_pyspark_dataframe,sales_validation,products_validation,stores_validation
from challenge_tasks.data_transformations import sales_aggregation,month_insights,enriched_data,price_range

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

# Task 2 - Data validation
df_sales=sales_validation(df=df_sales)
df_products=products_validation(df=df_products)
df_stores=stores_validation(df=df_stores)

## Part 2 - Tasks
# Task 1 - Sales Aggregation - 
sales_agg=sales_aggregation(df_sales=df_sales,df_product=df_products,)
# Task 2 - Monthly Sales Insights - 
monthly_sales_insights= month_insights(df_sales=df_sales,df_product=df_products)
# Task 3 - Enrich Data - 

enriched_dataframe = enriched_data(df_sales=df_sales, df_product=df_products, df_stores=df_stores, add_price_category=True)
for row in enriched_dataframe.take(5):
    print(row)

## Part 3 -Tasks
# Task 1 - Save Enrich Data into a parquet format partitioned by category and transaction_date
# Task 2 - Save revenue insights in CSV format


