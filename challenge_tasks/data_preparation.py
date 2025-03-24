"""
Script that contains the functions for data reading and validating data.
Function read_csv_into_pyspark_dataframe reads an csv file into a PYSPARK DATAFRAME.

"""
#Import necessary libraries

from pyspark.sql import SparkSession,DataFrame
import os
import logging
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,DateType
# import warnings
# from conf import settings
from pyspark.sql.functions import col,isnan, when, count,date_format,to_date, coalesce,trim
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
def sales_Validation(df : DataFrame) -> DataFrame:
    """
    Validates the data types against the sales columns in the dataframe
    Enforces the expected data types tothe sales dataframe
    Validates the schema of sales dataframe
    Check and remove null values on the dataframe
    
    Removes duplicates present in transaction_id column
    Check if the quantity and prices values are positive

    Args: 
    df (DataFrame) : sales dataframe

    Return:
    df (Dataframe) : Pyspark Dataframe with sales data validated
    """
    logger.info('Validating Sales Dataframe ...')

    # Expected schema for Sales Dataframe
    expected_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("transaction_date",DateType(), True),
    StructField("price", DoubleType(), True)
    ])

    # Creation of dictionarys column name and data type to compare schemas
    actual_schema = {field.name: field.dataType for field in df.schema}
    expected_schema_dict = {field.name: field.dataType for field in expected_schema}

    # Comparisson between the expcted schema and sales dataframe (order does not matter!) 
    if set(actual_schema.keys()) != set(expected_schema_dict.keys()):
            missing_columns = set(expected_schema_dict.keys()) - set(actual_schema.keys())
            extra_columns = set(actual_schema.keys()) - set(expected_schema_dict.keys())
            logging.error(f"Schemas do not match. Missing columns: {missing_columns}, Extra columns: {extra_columns}")
    else:
        logging.info("Schemas do match")

    # # Enforcing data types based on the expected schema 
    for field in expected_schema:
        if actual_schema[field.name] != field.dataType:
            logging.warning(f"Casting column {field.name} from {actual_schema[field.name]} to {field.dataType}")
            
            if isinstance(field.dataType, DateType): # Check if the field data type is DateType and process it to handle potential null values
                df = df.withColumn(
                    field.name,
                    coalesce(
                        to_date(col(field.name), "MMMM dd, yyyy"),
                        to_date(col(field.name), "yyyy-MM-dd"),
                        to_date(col(field.name), "yyyy/MM/dd"),
                        to_date(col(field.name), "MM/dd/yyyy"),
                        to_date(col(field.name), "dd-MM-yyyy"),
                        to_date(col(field.name), "dd/MM/yyyy")
                    )
                )
                df = df.withColumn(field.name, date_format(col(field.name), "yyyy-MM-dd"))
            else:
                df = df.withColumn(field.name, col(field.name).cast(field.dataType))
        
    # # Drop duplicates in  column transaction_id   
    for c in df.columns:
        df = df.filter(
            (~col(c).contains("None")) &  # Filter out 'None' as string
            (~col(c).contains("NULL")) &  # Filter out 'NULL' as string
            (trim(col(c)) != "") &  # Filter out empty strings
            (col(c).isNotNull())  # Remove actual nulls
        )
        
    # Drop duplicates in  column transaction_id   
    df=df.drop_duplicates(['transaction_id'])
    logging.info("Validation complete")

    return df