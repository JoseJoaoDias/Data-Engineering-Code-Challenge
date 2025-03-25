"""
Script for configurations aboout the app, mainly paths for the different folders.

"""

# Import important libraries
import os

# Base path to acess files
BASE_PATH = os.getcwd()

# Path to acess data folder
DATA_PATH = os.path.join(BASE_PATH, "data")

PRODUCTS_FILE = f"{DATA_PATH}/products_uuid.csv"
SALES_FILE = f"{DATA_PATH}/sales_uuid.csv"
STORES_FILE = f"{DATA_PATH}/stores_uuid.csv"

# Output Path
OUTPUT_PATH = f"{BASE_PATH}/output/"
OUTPUT_PATH_OPTIONAL = f"{BASE_PATH}/output/optional"

# Test Path Temporary
TEST_TEMP_PATH = f"{BASE_PATH}/temp_test/"
