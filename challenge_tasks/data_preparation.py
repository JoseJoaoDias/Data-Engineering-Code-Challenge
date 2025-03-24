"""
Script that contains the functions for data reading and validating data.
Function read_csv_into_pyspark_dataframe reads an csv file into a PYSPARK DATAFRAME.

"""
#Import necessary libraries

from pyspark.sql import SparkSession,DataFrame
import os
import logging
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

log_file = os.path.join(log_dir, "data_preparation.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO, # logging level can be adjusted if necessary (DEBUG, INFO, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s", # logging message format
)

logger=logging.getLogger(__name__)

# Create Spark Session
spark = SparkSession.builder \
    .appName("Data Preparations App") \
    .config("spark.hadoop.hadoop.native.lib", "false") \
    .master("local[*]") \
    .getOrCreate()

# Load the datsets products,sales and stores into Pyspark Dataframes
def read_csv_into_pyspark_dataframe(file_path: str) -> DataFrame:
    """
    Load a CSV file into a Pyspark Dataframe from a specific path.

    Args: 
    file_path (string) : path to the specific file

    Return:
    df (Dataframe) : Pyspark Dataframe with information from csv file.
    """
    logging.info('Loading csv file')
    df=spark.read.csv(file_path,
                   header=True,
                   inferSchema=True)
    logging.info(f'Dataset was loaded sucessfully from {file_path}. The dataset has Rows: {df.count()}, Columns: {len(df.columns)}')
    
    return df
