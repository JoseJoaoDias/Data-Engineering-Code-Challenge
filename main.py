"""
Main sript uses the functions from another scrips to perform the tasks.

"""
# Import libraries
# from pyspark.sql import SparkSession
import os
import logging
from conf import settings
from challenge_tasks.data_preparation import read_csv_into_pyspark_dataframe,validation#, spark  as spark_preparation
from challenge_tasks.data_transformations import sales_aggregation,month_insights,enriched_data
from challenge_tasks.data_export import export_dataframe_as_csv,export_dataframe_as_parquet_by_partitions

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

## Part 1 - Tasks
# Task 1 - Loaded 3 datasets
df_sales=read_csv_into_pyspark_dataframe(file_path=settings.SALES_FILE)
df_products=read_csv_into_pyspark_dataframe(file_path=settings.PRODUCTS_FILE)
df_stores=read_csv_into_pyspark_dataframe(file_path=settings.STORES_FILE)

# Task 2 - Data validation
df_sales=validation(df=df_sales,df_name='sales')
df_sales.show()
df_products=validation(df=df_products,df_name='products')
df_sales.show()
df_stores=validation(df=df_stores,df_name='stores')
df_sales.show()

## Part 2 - Tasks
# Task 1 - Sales Aggregation - Calculate the total revenue for each store (store_id) and each product category.
sales_agg=sales_aggregation(df_sales=df_sales,df_product=df_products,)
# Task 2 - Monthly Sales Insights - Calculate the total quantity sold for each product category, grouped by month.
monthly_sales_insights= month_insights(df_sales=df_sales,df_product=df_products)
# Task 3 - Enrich Data - Combine the sales, products, and stores datasets into a single enriched dataset

enriched_dataframe = enriched_data(df_sales=df_sales, df_product=df_products, df_stores=df_stores, add_price_category=False)
# Task 4 - Enrich Data with price range 
enriched_dataframe_category_price = enriched_data(df_sales=df_sales, df_product=df_products, df_stores=df_stores, add_price_category=True)

# ## Part 3 -Tasks
# Task 1 - Save Enrich Data into a parquet format partitioned by category and transaction_date
export_dataframe_as_parquet_by_partitions(
    df=enriched_dataframe, 
    df_name= 'enriched_dataframe',
    output_path = settings.OUTPUT_PATH, 
    partions = ["category","transaction_date"]
)

# # Task 2 - Save revenue insights in CSV format
export_dataframe_as_csv(df=sales_agg,
                        df_name='sales_agg',
                        output_path=settings.OUTPUT_PATH)

