"""
Script for exporting PySpark DataFrames to CSV and Parquet formats.

Functions included:
1. export_dataframe_as_csv - Saves a PySpark DataFrame as a single CSV file.
2. export_dataframe_as_parquet_by_partitions - Saves a PySpark DataFrame as a partitioned Parquet file.

This script also sets up logging and initializes a Spark session.
"""
#Import necessary libraries
from pyspark.sql import DataFrame
import os
import logging
import shutil
import glob

log_dir = os.path.abspath("logs")  


if not os.path.exists(log_dir):
    os.makedirs(log_dir)  # Creates directory if does not exists

log_file = os.path.join(log_dir, "data_export.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO, # logging level can be adjusted if necessary (DEBUG, INFO, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s", # logging message format
)

logger=logging.getLogger(__name__)

# # Create Spark Session
# spark = SparkSession.builder \
#     .appName("Data EXport App") \
#     .config("spark.hadoop.hadoop.native.lib", "false") \
#     .master("local[*]") \
#     .getOrCreate()

def export_dataframe_as_csv(df: DataFrame,df_name:str, output_path:str):
    """
    Exports a PySpark DataFrame as a single CSV file.

    Args:
        df (DataFrame): The PySpark DataFrame to be saved as a CSV file.
        output_path (str): The desired output file path (e.g., "final_output.csv").

    Returns:
        None
    """
    logger.info('Saving Csv file ...')

    # Creation of output path
    output_path_final=os.path.join(output_path, f"{df_name}.csv")

    temp_folder=output_path_final.replace(".csv","_temp")
    # Write as a single CSV file
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_folder)

    # Find the generated part file
    csv_file = glob.glob(os.path.join(temp_folder, "part-0000*.csv"))[0]  # Pick the correct file

    # Rename the part file to final_output.csv
    shutil.move(csv_file, f"{df_name}_final_output.csv")

    # Delete the temporary folder
    shutil.rmtree(temp_folder)
    logger.info('CSv file was saved.')


def export_dataframe_as_parquet_by_partitions(df : DataFrame, df_name : str, output_path:str, partions: list):
    """
    Exports a PySpark DataFrame as a partitioned Parquet file.

    Returns:
        None
    """
    # Creation of output path
    output_path=os.path.join(output_path, f"{df_name}.parquet")
    logger.info('Saving parquet file ...')
    df.write.mode("overwrite")\
            .partitionBy(partions)\
            .parquet(output_path)
    logger.info('Parquet file was saved.')
