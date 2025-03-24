"""
This script contains configuration settings for the app, including paths for various data folders 
and functions for data validation, transformation, and export.
"""
# Import important libraries

from pyspark.sql import SparkSession
from conf import settings
from challenge_tasks.data_preparation import read_csv_into_pyspark_dataframe,sales_validation,products_validation,stores_validation
from challenge_tasks.data_transformations import sales_aggregation,month_insights,enriched_data
from challenge_tasks.data_export import export_dataframe_as_csv,export_dataframe_as_parquet_by_partitions
import os
import csv
import shutil
import chispa

spark = SparkSession.builder \
    .appName("TestChallengApp") \
    .config("spark.hadoop.hadoop.native.lib", "false") \
    .master("local[*]") \
    .getOrCreate()


def create_mock_csv(path_test:str):
    # Creation of mock data
    mock_data = [
    ["store_id", "store_name", "location"],
    ["str_1", "Store A", "Location A"],
    ["str_2", "Store B", "Location B"]
    ]
    # Creation temp directory

    os.makedirs("temp", exist_ok=True)

    # Define file path
    file_path = os.path.join("temp", "mock_data.csv")

    # Write to CSV file
    with open(file_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(mock_data)
        
def test_read_csv_file():
    # Define file path
    test_path="test_path"
    # Creation of mock csv
    create_mock_csv(test_path)
    df=read_csv_into_pyspark_dataframe(test_path)
    # Expected Data
    expecetd_data = [
    ("str_1", "Store A", "Location A"),
    ("str_2", "Store B", "Location B")

    ]
    # Creation of mock csv
    expecetd_columns=["store_id","store_name","location"]
    expecetd_df=spark.createDataframe(expecetd_data,expecetd_columns)
    chispa.assert_df_equality(df,expecetd_df)


def test_sales_aggregation():

    # Create sample sales data
    df_sales = spark.createDataFrame([
        ("tra_1", "str_1", "pro_1", 5, "2024-11-01", 20.0),
        ("tra_2", "str_2", "pro_2", 10, "2024-11-02", 40.0),
        ("tra_3", "str_3", "pro_3", 15, "2024-12-03", 60.0),
        ("tra_4", "str_4", "pro_4", 20, "2024-12-04", 80.0)
    ], ["transaction_id", "store_id", "product_id", "quantity", "transaction_date", "price"])
    
    # Create sample product data
    df_products = spark.createDataFrame([
        ("pro_1", "Product A", "Category A"),
        ("pro_2", "Product B", "Category B")
    ], ["product_id", "product_name", "category"])
    # Sales aggregation data using method
    df_sales_agg= sales_aggregation(df_sales=df_sales,df_product=df_products)

    # Expected sales aggregation result
    expected_df_sales_agg = spark.createDataFrame([
        ("str_1", "Category A", 20.0),
        ("str_2", "Category B", 40.0)
    ], ["store_id", "category","total_revenue"])

    # Check that the output of sales aggregation data
    chispa.assert_df_equality(df_sales_agg, expected_df_sales_agg, ignore_column_order=True, ignore_nullable=True, ignore_row_order=True)


def test_month_insights():
    # Create sample sales data
    df_sales = spark.createDataFrame([
        ("tra_1", "str_1", "pro_1", 5, "2024-11-01", 20.0),
        ("tra_2", "str_2", "pro_2", 10, "2024-11-02", 40.0),
        ("tra_3", "str_3", "pro_3", 15, "2024-12-03", 60.0),
        ("tra_4", "str_4", "pro_4", 20, "2024-12-04", 80.0)
    ], ["transaction_id", "store_id", "product_id", "quantity", "transaction_date", "price"])
    
    # Create sample product data
    df_products = spark.createDataFrame([
        ("pro_1", "Product A", "Category A"),
        ("pro_2", "Product B", "Category B")
    ], ["product_id", "product_name", "category"])

    # Total quantity using method
    df_month_insigths = month_insights(df_sales=df_sales,df_product=df_products)

    # Expected Total quantity
    expected_df_month_insigths = spark.createDataFrame([
        ("2024", "12", "Category A", 20.0),
        ("2024", "11", "Category B", 40.0)
    ], ["year", "month", "category", "total_quantity_sold"])

    # Check that the output of month insights is correct
    chispa.assert_df_equality(df_month_insigths, expected_df_month_insigths, ignore_column_order=True, ignore_nullable=True, ignore_row_order=True)


def test_enriched_data():
    # Create sample sales data
    df_sales = spark.createDataFrame([
        ("tra_1", "str_1", "pro_1", 5, "2024-11-01", 20.0),
        ("tra_2", "str_2", "pro_2", 10, "2024-11-02", 40.0),
        ("tra_3", "str_3", "pro_3", 15, "2024-12-03", 60.0),
        ("tra_4", "str_4", "pro_4", 20, "2024-12-04", 80.0)
    ], ["transaction_id", "store_id", "product_id", "quantity", "transaction_date", "price"])
    # Create sample product data
    df_products = spark.createDataFrame([
        ("pro_1", "Product A", "Category A"),
        ("pro_2", "Product B", "Category B")
    ], ["product_id", "product_name", "category"])

    # Create sample stores data
    df_stores = spark.createDataFrame([
        ("str_1", "Store A", "Location A"),
        ("str_2", "Store B", "Location B"),
        ("str_3", "Store C", "Location C")
    ], ["store_id", "store_name", "location"])



    # Enrich data without price category using method
    df_enriched = enriched_data(df_products, df_stores, df_sales, add_price_category=False)

    # Expected enriched result without price category
    expected_df_enriched = spark.createDataFrame([
        ("t1", "Product A", "Category X", "Store A", "Location X", 5, "2024-12-01", 20.0),
        ("t2", "Product B", "Category Y", "Store B", "Location Y", 10, "2024-12-02", 40.0)
    ], ["transaction_id", "product_name", "category", "store_name", "location", "quantity", "transaction_date", "price"])

    # Check that the output of enriched data without price category is correct
    chispa.assert_df_equality(df_enriched, expected_df_enriched, ignore_column_order=True, ignore_nullable=True, ignore_row_order=True)



def test_export_dataframe_as_parquet_by_partitions():
     # Create a DataFrame to be written
    df = spark.createDataFrame([
        ("t1", "Product A", "Category X", "Store A", "Location X", 5, "2024-12-01", 20.0),
        ("t2", "Product B", "Category Y", "Store B", "Location Y", 10, "2024-12-02", 40.0)
    ], ["transaction_id", "product_name", "category", "store_name", "location", "quantity", "transaction_date", "price"])

    # Path where the Parquet file will be saved
    output_path = 'tmp/test_parquet_output.parquet'
    
    # Export the DataFrame to a Parquet file
    export_dataframe_as_parquet_by_partitions(df, output_path, partitions_list=["product_name","category"])

    # Read back the Parquet file into a DataFrame
    df_read_back = spark.read.parquet(output_path)

    # Compare the DataFrame that was written and the DataFrame read back
    chispa.assert_df_equality(df, df_read_back, ignore_column_order=True, ignore_nullable=True, ignore_row_order=True),

    # Clean up temporary files
    if os.path.exists('tmp'):
        shutil.rmtree('tmp')
        print("Cleaned up test files.")

def test_export_dataframe_as_csv():
    # Create a DataFrame to be written
    df = spark.createDataFrame([
        ("t1", "Product A", "Category X", "Store A", "Location X", 5, "2024-12-01", 20.0),
        ("t2", "Product B", "Category Y", "Store B", "Location Y", 10, "2024-12-02", 40.0)
    ], ["transaction_id", "product_name", "category", "store_name", "location", "quantity", "transaction_date", "price"])

    # Path where the CSV file will be saved
    output_path = 'tmp/test_csv_output.csv'

    # Export the DataFrame to a CSV file
    export_dataframe_as_csv(df, output_path)

    # Read back the CSV file into a DataFrame
    df_read_back = spark.read.option("header", "true").csv(output_path)

    # Compare the DataFrame that was written and the DataFrame read back
    chispa.assert_df_equality(df, df_read_back, ignore_column_order=True, ignore_nullable=True, ignore_row_order=True)

    # Manually perform cleanup after running tests
    if os.path.exists('tmp'):
        shutil.rmtree('tmp')
        print("Cleaned up test files.")


