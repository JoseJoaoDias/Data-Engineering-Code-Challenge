"""
Script for transforming sales and product data using PySpark.

Functions included:
1. read_csv_into_pyspark_dataframe - Reads a CSV file into a PySpark DataFrame.
2. sales_aggregation - Computes total revenue per store and product category.
3. month_insights - Calculates total quantity sold per product category and month.
4. price_range - UDF to classify prices into 'Low', 'Medium', or 'High' categories.
5. enriched_data - Merges sales, product, and store data, adding an optional price category.

This script also sets up logging and initializes a Spark session.
"""
#Import necessary libraries

from pyspark.sql import DataFrame
import os
import logging
from pyspark.sql.types import StringType
from pyspark.sql import functions as F



## Setup logging configuration
# Path to logs file
log_dir = os.path.abspath("logs")  


if not os.path.exists(log_dir):
    os.makedirs(log_dir)  # Creates directory if does not exists

log_file = os.path.join(log_dir, "data_transformation.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO, # logging level can be adjusted if necessary (DEBUG, INFO, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s", # logging message format
)

logger=logging.getLogger(__name__)

# # Create Spark Session
# spark = SparkSession.builder \
#     .appName("Data Transformation App") \
#     .config("spark.hadoop.hadoop.native.lib", "false") \
#     .master("local[*]") \
#     .getOrCreate()

def sales_aggregation(df_product : DataFrame, df_sales: DataFrame) -> DataFrame:
    """
    Aggregates sales data to calculate the total revenue per store and product category.  

    Args:  
    df_product (DataFrame): Validated product dataset.  
    df_sales (DataFrame): Validated sales dataset.  

    Return:  
    df_result (DataFrame): Aggregated PySpark DataFrame with total revenue per store and category
    """
    logger.info('Computing total revenue by store and category ...')

    # Total revenue computation
    df_result = df_sales.join(df_product, on="product_id", how="left") \
        .groupBy(df_sales.store_id, df_product.category) \
        .agg(F.sum('price').alias('total_revenue'))
    
    # Filtering null values that may exist
    df_result = df_result.filter(F.col("category").isNotNull())
    logger.info('Computing total revenue by store and category was successful')

    return df_result

def month_insights(df_product : DataFrame, df_sales: DataFrame) -> DataFrame:
    """
    Calculates the total quantity for each product category and month
   

    Args: 
    df_product (DataFrame) : Product validated dataframe
    df_sales (DataFrame) : Sales validated dataframe


    Return:
    df_result (DataFrame): Aggregated PySpark DataFrame with total quantity per category and month
    """
    logger.info('Computing total quantity by category and month ...')

    # Total quantity computation
    df_result=df_sales.join(df_product, on="product_id", how="left") \
    .groupBy(F.year('transaction_date').alias('year'), F.month('transaction_date').alias('month'), df_product.category) \
    .agg(F.sum('quantity').alias('total_quantity_sold'))
    
    # Filtering null values that may exist
    df_result = df_result.filter(F.col("category").isNotNull())

    logger.info('Computing total quantity by category and month was successful')

    return df_result

@F.udf(StringType())  # Register the UDF with return type as String
def price_range(price):
    """
    Classifies the price into different ranges.

    Args:
        price (float): The price value to classify.

    Returns:
        str: The price range classification - 'Low', 'Medium', or 'High'.
    """
    # if condition for price
    logger.info('Calculating price range...')

def price_range(price: float) -> str:
    """
    Classifies the price into different ranges.

    Args:
        price (float): The price value to classify.

    Returns:
        str: The price range classification - 'Low', 'Medium', or 'High'.
    """
    if price < 20:
        return 'Low'
    elif 20 <= price <= 100:
        return 'Medium'
    else:
        return 'High'

    
def enriched_data(df_product : DataFrame, df_sales: DataFrame,df_stores: DataFrame,add_price_category: bool) -> DataFrame:
    """
    Calculates the total revenue for each store and each product category
   

    Args: 
    df_product (DataFrame) : Product validated dataframe
    df_sales (DataFrame) : Sales validated dataframe


    Return:
    df_result (DataFrame): Aggregated PySpark DataFrame with total quantity per category and month
    """
    logger.info('Creating enriched dataset...')
    # Creation of enriched dataset
    df_enriched=df_sales.join(df_product,on='product_id', how="left") \
    .join(df_stores, on='store_id', how="left") \
    .select('transaction_id', 'store_name','location','product_name','category','quantity','transaction_date','price')
   
    # Creation of price category based if specified
    if add_price_category:
        price_range_udf = F.udf(price_range, StringType())
        df_enriched = df_enriched.withColumn('price_category', price_range_udf(df_enriched["price"]))
    
    # Drop Nulls if any
    df_result = df_enriched.filter(F.col("store_name").isNotNull())\
                          .filter(F.col("location").isNotNull())\
                          .filter(F.col("product_name").isNotNull())\
                          .filter(F.col("category").isNotNull())
    logger.info('Enriched dataset was created')

    return df_result