import os
import pandas as pd

# get insights directory
TEST_FILES_DIR = os.path.dirname(__file__)
CSV_FILES = os.walk(TEST_FILES_DIR, topdown=True)
print(f"Converting csv files in {TEST_FILES_DIR} to Parquet format...")

for root, dirs, files in CSV_FILES:
    for csv_file in files:
        if csv_file.endswith(".csv"):
            csv_file = os.path.join(root, csv_file)
            print(f"Processing file: {csv_file}")
            # set dtypes for cv file to avoid dtype inference issues
            df = pd.read_csv(csv_file)
            parquet_file_path = os.path.splitext(csv_file)[0] + ".parquet"
            df.to_parquet(parquet_file_path, index=False, engine="pyarrow")
            print(f"Converted {csv_file} to {parquet_file_path}")
