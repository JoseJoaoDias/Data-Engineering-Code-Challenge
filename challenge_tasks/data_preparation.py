"""
Script for reading and validating datasets (sales, products, and stores) in PySpark.  
It includes functions to:  
- Load a CSV file into a PySpark DataFrame.  
- Validate schemas and enforce correct data types.  
- Handle missing or null values.  
- Remove duplicates based on unique identifiers.  
- Ensure data consistency for further processing.  

"""
#Import necessary libraries

from pyspark.sql import DataFrame
import os
import logging
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,DateType
from pyspark.sql.functions import col,date_format,to_date, coalesce,trim, upper
from challenge_tasks.spark_session import spark

## Setup logging configuration
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

def validation(df : DataFrame, df_name:str) -> DataFrame:
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
    expected_schema_sales = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("transaction_date",DateType(), True),
    StructField("price", DoubleType(), True)
    ])

    expected_schema_product = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True)
    ])


    expected_schema_store = StructType([
    StructField("store_id", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("location", StringType(), True)
    ])


    if df_name== 'sales':
         expected_schema=expected_schema_sales
    elif df_name=='products':
        expected_schema=expected_schema_product
    else :
        expected_schema=expected_schema_store

    # Creation of dictionarys column name and data type to compare schemas
    actual_schema = {field.name: field.dataType for field in df.schema}
    expected_schema_dict = {field.name: field.dataType for field in expected_schema}

    # Comparisson between the expcted schema and sales dataframe (order does not matter!) 
    if set(actual_schema.keys()) != set(expected_schema_dict.keys()):
            missing_columns = set(expected_schema_dict.keys()) - set(actual_schema.keys())
            extra_columns = set(actual_schema.keys()) - set(expected_schema_dict.keys())
            logging.error(f"Schemas do not match for {df_name} dataset. Missing columns: {missing_columns}, Extra columns: {extra_columns}")
    else:
        logging.info("Schemas do match for {df_name} dataset.")

    # Enforcing data types based on the expected schema 
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
                df = df.withColumn(field.name, date_format(col(field.name), "yyyy-MM-dd").cast(field.dataType))
            else:
                df = df.withColumn(field.name, col(field.name).cast(field.dataType))
        
    # Assess id columns and columns that will have the nulls values removed
    id_cols = [c for c in df.columns if "id" in c.lower()]
    cols_to_clean= [c for c in df.columns if "id" in c.lower()]
    
    # for sales dataset defines the id columns and columns to be cleaned from null values
    if df_name=='sales':
        cols_to_clean = df.columns
        id_cols=['transaction_id']
    # Cleaning null values
    for c in cols_to_clean:
        df = df.filter(
            (trim(col(c)) != "") &                                    # Remove empty strings
            (~upper(trim(col(c))).contains("NULL")) &                 # Remove case-insensitive 'NULL'
            (~upper(trim(col(c))).contains("NONE")) &                   # Remove case-insensitive 'NONE'
            (col(c).isNotNull())             # Remove actual nulls
        )            
    # Drop duplicates for id columns in Sales all ids, in stores store_id and in products product id
    df=df.drop_duplicates(id_cols)
        
    # Guarantee in sales that the price and quantity are bigger than 0
    if df_name=='sales':
            df = df.filter(
                (col("quantity") >= 0) & 
                (col("price") >= 0)
            )
    else:
        non_id_cols = [c for c in df.columns if "id" not in c.lower()] # non id columns for stores and products
        df=df.drop_duplicates(non_id_cols) # drop of duplicates combination of products and store dataframe

    # Drop duplicates in  column transaction_id if any
    logging.info("Validation complete")

    return df
