# Data Processing Pipeline Documentation

## Project Overview:
This project processes, analysis and infer some insights from raw data related to sales, products and stores. Trough the use of a data pipeline in Pyspark will be able to infer severall insigths regarding the data, producing two output files, one in Parquet formatt and another in CSV formatt. The process loads, validates and transforms the data to achieve teh final result.


## Approach to the problem:
 ### Part 1 -  Data Preparation:
    - Loading the raw CSV files for sales, products and stores into Pyspark DataFrames.
    - Ensure the Dataframes have the the right schema, and data types.
    - Ensure the null and duplicated values are eliminated.
 ### Part 2 -  Data Transformation:
    - Aggregating data to compute total revenue by store_id and category.
    - Aggregating data to compute total quantity by month and category.
    - Creation of an enriched dataframe that contains information from all the dataframes.
    - Possibility to categorize the price based on rules.
 ### Part 3: Data Export:
    - Exporting monthly insights (total quantity by month and category) as CSV file.
    - Exporting enriched dataframe as Parquet file.



## Assumptions and Decisions.
 ### Assumptions.
  Environment: The code is assumed to be executed within a PySpark environment with access to necessary libraries.
  Data Format: The CSV files used as input are formatted correctly with headers matching the expected structure.
  Spark Configuration: A SparkSession instance must be initialised for the pipeline to run.

### Decisions:
Data Validation

    Static Schemas: Defined and enforced for the products, stores, and sales datasets based on the challenge specifications.

    Data Type Enforcement: Data types for all columns are validated against the defined schemas to ensure consistency during processing.

Data Quality

    Missing Values: Rows containing null values in any critical fields are dropped during preprocessing.

    Duplicate Handling: Duplicates are removed from all datasets using their unique identifiers 

Price Categorization (Optional)

    Functionality: A dedicated function was implemented to generate a price category (Low, Medium, High) as an optional enrichment step.

    Optional Output: This step is configurable and can be enabled or disabled depending on the use case.

Data Export

    Output Directory: All output files, including cleaned and enriched datasets, are saved to the /output/ directory.


### Prerequisites
Ensure you have installed the required libraries by running:

            pip install -r requirements.txt

Running the Code
Prepare the data: Ensure that the raw data files (sales_uuid.csv, products_uuid.csv, stores_uuid.csv) are available in the data directory.
Run the main script: The main script, located in main.py, orchestrates the entire pipeline. Run it using:

python main.py

### Testing: To test the individual classes and methods, run pytest on the test files located in the tests directory:

pytest tests/

Expected Output

    The output data is stored in the output_data directory. The transformed data is saved in various formats such as CSV and Parquet.
    Logs generated during execution can be found in the console or in a specified log file for debugging purposes.

### File Structure




