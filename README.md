# Data Processing Pipeline Documentation

## Project Overview:
This project processes, analysis and infer some insights from raw data related to sales, products and stores. Trough the use of a data pipeline in Pyspark will be able to infer several insights regarding the data, producing two output files, one in Parquet format and another in CSV format. The process loads, validates and transforms the data to achieve the final result.

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
 ### Assumptions:
  - Data Format : The CSV files used as input are formatted correctly with headers matching the expected structure.

### Decisions:
**Sesssion**

    - Spark Session: Creation of file to start Spark Session.

**Data Validation**

    - Static Schemas: Defined and enforced for the products, stores, and sales datasets based on the challenge specifications.

    - Data Type Enforcement: Data types for all columns are validated against the defined schemas to ensure consistency during processing.

**Data Quality**

    - Missing Values: Rows containing null values in any critical fields are dropped during preprocessing (Primary and Foreign Keys).
    
    - Rules for Missing Values: Null Values were removed from all columns of Sales Dataframe.

    - Duplicate Handling: Duplicates are removed from all datasets using their unique identifiers 
        
    - Rules for Validation: Price and Quantity must be higher than 0.

**Price Categorization (Optional)**

    - Functionality: A dedicated function was implemented to generate a price category (Low, Medium, High) as an optional enrichment step.

    - Optional Output: This step is configurable and can be enabled or disabled depending on the use case.

**Data Export**

    Output Directory: All output files, including revenue insights and enriched dataset, are saved to the /output/ directory.
    Output Optional Directory: Optional output file is saved to the /output/optional directory.

## File Structure
```
DATA-ENGINEERING-CODE-CHALLENGE/ │-- .github/ │ ├── workflows/ │ │ ├── ci.yml │-- challenge_tasks/ │ ├── init.py │ ├── data_export.py │ ├── data_preparation.py │ ├── data_transformations.py │ ├── spark_session.py │-- conf/ │ ├── init.py │ ├── settings.py │-- data/ │ ├── products_uuid.csv │ ├── sales_uuid.csv │ ├── stores_uuid.csv │-- logs/ │-- output/ │-- .gitignore │-- main.py │-- README.md │-- requirements.txt │-- tests.py
```
## Code Execution
Install required libraries using the command:
```
pip install -r requirements.txt
```

Run the main script using the command:
```
python main.py
```
The main.py file orchestrates the entire pipeline.

Note: The csv files must be in the data folder.
## Testing

To test the application run the command:
```
pytest tests.py
```

## Expected Output
## 📂 Output Folder Structure

The `output/` folder will contain the following files:

- **`enriched_dataframe`**  
  - Stored in **Parquet format**  
  - Partitioned by:
    - `category`
    - `transaction_date`  

- **`revenue_insights.csv`**  
  - Stored in **CSV format**  
  - Contains revenue insights and aggregated data  

- **`optional/enriched_dataframe_category_price`**  
  - Stored in **Parquet format**  
  - Partitioned by:
    - `category`
    - `transaction_date`  
