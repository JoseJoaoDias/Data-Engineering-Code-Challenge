"""
This script contains configuration settings for the app, including paths for various data folders
and functions for data validation, transformation, and export.
"""

# Import important libraries

from pyspark.sql import SparkSession
from conf import settings
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StructType,
    StructField,
    StringType,
    DoubleType,
    DateType,
)
from challenge_tasks.data_preparation import read_csv_into_pyspark_dataframe, validation
from challenge_tasks.data_transformations import (
    sales_aggregation,
    month_insights,
    enriched_data,
)
from challenge_tasks.data_export import (
    export_dataframe_as_csv,
    export_dataframe_as_parquet_by_partitions,
)
import os
import csv
import shutil
import chispa
import datetime
import logging

# Deactivate logs
logging.disable(logging.CRITICAL)

spark = (
    SparkSession.builder.appName("TestChallengApp")
    .config("spark.hadoop.hadoop.native.lib", "false")
    .master("local[*]")
    .getOrCreate()
)


def test_read_csv_file():

    # Creation of mock data
    mock_data = [
        ["store_id", "store_name", "location"],
        ["str_1", "Store A", "Location A"],
        ["str_2", "Store B", "Location B"],
    ]

    # Creation temp directory
    os.makedirs(test_path, exist_ok=True)

    # Define file path
    file_path = os.path.join(test_path, "mock_data.csv")

    # Write to CSV file
    with open(file_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(mock_data)

    # Define file path
    test_path = settings.TEST_TEMP_PATH
    # Creation of mock csv
    df = read_csv_into_pyspark_dataframe(test_path)
    # Expected Data
    expecetd_data = [
        ("str_1", "Store A", "Location A"),
        ("str_2", "Store B", "Location B"),
    ]
    # Creation of mock csv
    expecetd_columns = ["store_id", "store_name", "location"]
    expecetd_df = spark.createDataFrame(expecetd_data, expecetd_columns)
    chispa.assert_df_equality(df, expecetd_df)


def test_sales_validation():
    # Create sample sales data
    df = spark.createDataFrame(
        [
            ("tra_1", "str_1", "pro_1", 5, "2024-11-01", 20.0),
            ("tra_2", None, "pro_2", 10, "2024-11-02", 40.0),
            ("tra_3", "Null", "pro_4", 15, "2024-11-04", 60.0),
            ("tra_4", "None", "pro_4", 20, "2024-11-05", 80.0),
            ("tra_5", "", "pro_4", 25, "2024-11-06", 100.0),
            ("tra_6", "str_2", None, 30, "2024-11-07", 120.0),
            ("tra_7", "str_2", "Null", 35, "2024-11-08", 140.0),
            ("tra_8", "str_2", "None", 40, "2024-11-09", 160.0),
            ("tra_9", "str_2", "", 45, "2024-11-10", 170.0),
            (None, "str_3", "pro_4", 50, "2024-11-11", 180.0),
            ("Null", "str_3", "pro_4", 55, "2024-11-12", 190.0),
            ("None", "str_3", "pro_4", 60, "2024-11-13", 200.0),
            ("tra_10", "str_3", "pro_4", 65, "2024-11-14", 220.0),
            ("tra_10", "str_3", "pro_5", 70, "2024-11-15", 240.0),
            ("tra_11", "str_4", "pro_5", 75, "2024-11-16", 260.0),
            ("tra_12", "str_4", "pro_5", 80, "2024-11-16", -260.0),
            ("tra_13", "str_4", "pro_5", -80, "2024-11-16", 280.0),
        ],
        [
            "transaction_id",
            "store_id",
            "product_id",
            "quantity",
            "transaction_date",
            "price",
        ],
    )

    # Validated data using method
    df_validation = validation(df=df, df_name="sales")
    df_validation.show()
    df_validation.printSchema()
    # Create expected dataframe
    schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("store_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("transaction_date", DateType(), True),
            StructField("price", DoubleType(), True),
        ]
    )

    expected_df_validation = spark.createDataFrame(
        [
            ("tra_1", "str_1", "pro_1", 5, datetime.date(2024, 11, 1), 20.0),
            ("tra_10", "str_3", "pro_4", 65, datetime.date(2024, 11, 14), 220.0),
            ("tra_11", "str_4", "pro_5", 75, datetime.date(2024, 11, 16), 260.0),
        ],
        schema,
    )

    expected_df_validation.show()
    expected_df_validation.printSchema()

    # Check that the output of sales data validation is correct
    chispa.assert_df_equality(
        df_validation,
        expected_df_validation,
        ignore_column_order=True,
        ignore_nullable=True,
        ignore_row_order=True,
    )


def test_products_validation():

    # Create sample product data
    df = spark.createDataFrame(
        [
            ("pro_1", "Product A", "Category A"),
            ("None", "Product B", "Category B"),
            ("Null", "Product B", "Category B"),
            (None, "Product B", "Category B"),
            ("pro_1", "Product B", "Category B"),
            ("pro_2", "Product B", "Category B"),
            ("pro_3", "Product C", "Category C"),
        ],
        ["product_id", "product_name", "category"],
    )

    # Validated data using method
    df_validation = validation(df=df, df_name="products")

    # Create expected dataframe
    schema = StructType(
        [
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
        ]
    )
    expected_df_validation = spark.createDataFrame(
        [
            ("pro_1", "Product A", "Category A"),
            ("pro_2", "Product B", "Category B"),
            ("pro_3", "Product C", "Category C"),
        ],
        schema,
    )

    # Check that the output of products data validation is correct
    chispa.assert_df_equality(
        df_validation,
        expected_df_validation,
        ignore_column_order=True,
        ignore_nullable=True,
        ignore_row_order=True,
    )


def test_store_validation():
    # Create sample stores data
    df = spark.createDataFrame(
        [
            ("str_1", "Store A", "Location A"),
            ("None", "Store A", "Location A"),
            ("Null", "Store A", "Location A"),
            ("null ", "Store A", "Location A"),
            (None, "Store B", "Location B"),
            ("str_3", "Store C", "Location C"),
            ("str_3", "Store C", "Location C"),
        ],
        ["store_id", "store_name", "location"],
    )

    # Validated data using method
    df_validation = validation(df=df, df_name="stores")

    # Create expected dataframe
    schema = StructType(
        [
            StructField("store_id", StringType(), True),
            StructField("store_name", StringType(), True),
            StructField("location", StringType(), True),
        ]
    )
    expected_df_validation = spark.createDataFrame(
        [("str_1", "Store A", "Location A"), ("str_3", "Store C", "Location C")], schema
    )

    # Check that the output of stores data validation is correct
    chispa.assert_df_equality(
        df_validation,
        expected_df_validation,
        ignore_column_order=True,
        ignore_nullable=True,
        ignore_row_order=True,
    )


def test_sales_aggregation():

    # Create sample sales data
    df_sales = spark.createDataFrame(
        [
            ("tra_1", "str_1", "pro_1", 5, "2024-11-01", 20.0),
            ("tra_2", "str_2", "pro_2", 10, "2024-11-02", 40.0),
            ("tra_3", "str_3", "pro_3", 15, "2024-12-03", 60.0),
            ("tra_4", "str_4", "pro_4", 20, "2024-12-04", 80.0),
        ],
        [
            "transaction_id",
            "store_id",
            "product_id",
            "quantity",
            "transaction_date",
            "price",
        ],
    )

    # Create sample product data
    df_products = spark.createDataFrame(
        [("pro_1", "Product A", "Category A"), ("pro_2", "Product B", "Category B")],
        ["product_id", "product_name", "category"],
    )
    # Sales aggregation data using method
    df_sales_agg = sales_aggregation(df_sales=df_sales, df_product=df_products)

    # Expected sales aggregation result
    expected_df_sales_agg = spark.createDataFrame(
        [("str_1", "Category A", 20.0), ("str_2", "Category B", 40.0)],
        ["store_id", "category", "total_revenue"],
    )

    # Check that the output of sales aggregation data
    chispa.assert_df_equality(
        df_sales_agg,
        expected_df_sales_agg,
        ignore_column_order=True,
        ignore_nullable=True,
        ignore_row_order=True,
    )


def test_month_insights():
    # Create sample sales data
    df_sales = spark.createDataFrame(
        [
            ("tra_1", "str_1", "pro_1", 5, "2024-11-01", 20.0),
            ("tra_2", "str_2", "pro_1", 10, "2024-11-02", 40.0),
            ("tra_3", "str_3", "pro_2", 15, "2024-12-03", 60.0),
            ("tra_4", "str_4", "pro_2", 20, "2024-12-04", 80.0),
        ],
        [
            "transaction_id",
            "store_id",
            "product_id",
            "quantity",
            "transaction_date",
            "price",
        ],
    )

    # Create sample product data
    df_products = spark.createDataFrame(
        [("pro_1", "Product A", "Category A"), ("pro_2", "Product B", "Category B")],
        ["product_id", "product_name", "category"],
    )

    # Total quantity using method
    df_month_insigths = month_insights(df_sales=df_sales, df_product=df_products)

    schema = StructType(
        [
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("category", StringType(), True),
            StructField("total_quantity_sold", LongType(), True),  # Change to LongType
        ]
    )

    expected_df_month_insigths = spark.createDataFrame(
        [(2024, 11, "Category A", 15), (2024, 12, "Category B", 35)], schema
    )

    # Check that the output of month insights is correct
    chispa.assert_df_equality(
        df_month_insigths,
        expected_df_month_insigths,
        ignore_column_order=True,
        ignore_nullable=True,
        ignore_row_order=True,
    )


def test_enriched_data():
    # Create sample sales data
    df_sales = spark.createDataFrame(
        [
            ("tra_1", "str_1", "pro_1", 5, "2024-11-01", 20.0),
            ("tra_2", "str_2", "pro_2", 10, "2024-11-02", 40.0),
            ("tra_3", "str_3", "pro_3", 15, "2024-12-03", 60.0),
            ("tra_4", "str_4", "pro_4", 20, "2024-12-04", 80.0),
        ],
        [
            "transaction_id",
            "store_id",
            "product_id",
            "quantity",
            "transaction_date",
            "price",
        ],
    )
    # Create sample product data
    df_products = spark.createDataFrame(
        [("pro_1", "Product A", "Category A"), ("pro_2", "Product B", "Category B")],
        ["product_id", "product_name", "category"],
    )

    # Create sample stores data
    df_stores = spark.createDataFrame(
        [
            ("str_1", "Store A", "Location A"),
            ("str_2", "Store B", "Location B"),
            ("str_3", "Store C", "Location C"),
        ],
        ["store_id", "store_name", "location"],
    )

    # Enrich data without price category using method
    df_enriched = enriched_data(
        df_product=df_products,
        df_sales=df_sales,
        df_stores=df_stores,
        add_price_category=False,
    )

    schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("store_name", StringType(), True),
            StructField("location", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("quantity", LongType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("price", DoubleType(), True),
        ]
    )

    expected_df_enriched = spark.createDataFrame(
        [
            (
                "tra_1",
                "Store A",
                "Location A",
                "Product A",
                "Category A",
                5,
                "2024-11-01",
                20.0,
            ),
            (
                "tra_2",
                "Store B",
                "Location B",
                "Product B",
                "Category B",
                10,
                "2024-11-02",
                40.0,
            ),
        ],
        schema,
    )

    # Check that the output of enriched data without price category is correct
    chispa.assert_df_equality(
        df_enriched,
        expected_df_enriched,
        ignore_column_order=True,
        ignore_nullable=True,
        ignore_row_order=True,
    )


def test_enriched_data_price_range():
    # Create sample sales data
    df_sales = spark.createDataFrame(
        [
            ("tra_1", "str_1", "pro_1", 5, "2024-11-01", 10.0),
            ("tra_2", "str_2", "pro_2", 10, "2024-11-02", 40.0),
            ("tra_3", "str_3", "pro_3", 30, "2024-12-01", 160.0),
            ("tra_4", "str_4", "pro_4", 60, "2024-12-02", 640.0),
        ],
        [
            "transaction_id",
            "store_id",
            "product_id",
            "quantity",
            "transaction_date",
            "price",
        ],
    )
    # Create sample product data
    df_products = spark.createDataFrame(
        [
            ("pro_1", "Product A", "Category A"),
            ("pro_2", "Product B", "Category B"),
            ("pro_3", "Product C", "Category C"),
        ],
        ["product_id", "product_name", "category"],
    )

    # Create sample stores data
    df_stores = spark.createDataFrame(
        [
            ("str_1", "Store A", "Location A"),
            ("str_2", "Store B", "Location B"),
            ("str_3", "Store C", "Location C"),
        ],
        ["store_id", "store_name", "location"],
    )

    # Enrich data without price category using method
    df_enriched = enriched_data(
        df_product=df_products,
        df_sales=df_sales,
        df_stores=df_stores,
        add_price_category=True,
    )
    schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("store_name", StringType(), True),
            StructField("location", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("quantity", LongType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("price_category", StringType(), True),
        ]
    )

    expected_df_enriched = spark.createDataFrame(
        [
            (
                "tra_1",
                "Store A",
                "Location A",
                "Product A",
                "Category A",
                5,
                "2024-11-01",
                10.0,
                "Low",
            ),
            (
                "tra_2",
                "Store B",
                "Location B",
                "Product B",
                "Category B",
                10,
                "2024-11-02",
                40.0,
                "Medium",
            ),
            (
                "tra_3",
                "Store C",
                "Location C",
                "Product C",
                "Category C",
                30,
                "2024-12-01",
                160.0,
                "High",
            ),
        ],
        schema,
    )

    # Check that the output of enriched data without price category is correct
    chispa.assert_df_equality(
        df_enriched,
        expected_df_enriched,
        ignore_column_order=True,
        ignore_nullable=True,
        ignore_row_order=True,
    )


def test_export_dataframe_as_parquet_by_partitions():
    # Create a DataFrame to be written
    df = spark.createDataFrame(
        [
            (
                "t1",
                "Store A",
                "Location A",
                "Product A",
                "Category A",
                5,
                "2024-12-01",
                20.0,
            ),
            (
                "t2",
                "Store B",
                "Location B",
                "Product B",
                "Category B",
                10,
                "2024-12-02",
                40.0,
            ),
        ],
        [
            "transaction_id",
            "store_name",
            "location",
            "product_name",
            "category",
            "quantity",
            "transaction_date",
            "price",
        ],
    )

    # Path where the Parquet file will be saved
    output_path = "temp_test/test_parquet_output.parquet"

    # Export the DataFrame to a Parquet file
    export_dataframe_as_parquet_by_partitions(
        df=df,
        df_name="test_csv_output",
        output_path=output_path,
        partions=["product_name", "category"],
    )

    # Read back the Parquet file into a DataFrame
    df_read_back = spark.read.parquet(output_path)

    # Compare the DataFrame that was written and the DataFrame read back
    chispa.assert_df_equality(
        df,
        df_read_back,
        ignore_column_order=True,
        ignore_nullable=True,
        ignore_row_order=True,
    ),

    # Clean up temporary files
    if os.path.exists(settings.TEST_TEMP_PATH):
        shutil.rmtree(settings.TEST_TEMP_PATH)


def test_export_dataframe_as_csv():
    # Create a DataFrame to be written
    schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("store_name", StringType(), True),
            StructField("location", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("quantity", StringType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("price", StringType(), True),
        ]
    )

    df = spark.createDataFrame(
        [
            (
                "tra_1",
                "Store A",
                "Location A",
                "Product A",
                "Category A",
                "5",
                "2025-03-01",
                "10.0",
            ),
            (
                "tra_2",
                "Store B",
                "Location B",
                "Product B",
                "Category B",
                "10",
                "2025-03-02",
                "40.0",
            ),
            (
                "tra_3",
                "Store C",
                "Location C",
                "Product C",
                "Category C",
                "30",
                "2025-03-04",
                "160.0",
            ),
        ],
        schema,
    )

    # Path where the CSV file will be saved
    output_path = settings.TEST_TEMP_PATH

    # Export the DataFrame to a CSV file
    export_dataframe_as_csv(df=df, df_name="test_csv", output_path=output_path)

    # Check if the path exists
    output_file = os.path.join(output_path, "test_csv.csv")
    assert os.path.exists(output_file), f"CSV file was not created at {output_file}"

    # Read back the CSV file into a DataFrame
    df_read_back = spark.read.option("header", "true").csv(
        os.path.join(output_path, "test_csv.csv")
    )

    # Compare the DataFrame that was written and the DataFrame read back
    chispa.assert_df_equality(
        df,
        df_read_back,
        ignore_column_order=True,
        ignore_nullable=True,
        ignore_row_order=True,
    )

    # Manually perform cleanup after running tests
    if os.path.exists(settings.TEST_TEMP_PATH):
        shutil.rmtree(settings.TEST_TEMP_PATH)


# Enable logging
logging.disable(logging.NOTSET)
