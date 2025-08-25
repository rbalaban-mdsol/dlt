import boto3
import pandas as pd
import numpy as np
import time
import os
import dlt
from pathlib import Path

DB_NAME = "sandbox_static"
TABLE_NAME = "dlt_demo"

@dlt.source
def parquet_files_source(folder_path: str = "."):
    """
    DLT source that reads all parquet files from a specified folder.
    Defaults to the current directory.
    """
    @dlt.resource(name=TABLE_NAME, write_disposition="append")
    def parquet_reader():
        """
        DLT resource that yields data from parquet files found in the folder.
        The resource name will be used as the table name in Athena.
        """
        folder = Path(folder_path)
        parquet_files = list(folder.glob("*.parquet"))

        if not parquet_files:
            print(f"No parquet files found in folder {folder_path}")
            return

        print(f"Found parquet files: {[p.name for p in parquet_files]}")
        for file_path in parquet_files:
            df = pd.read_parquet(file_path)
            yield df.to_dict(orient="records")

    return parquet_reader


if __name__ == "__main__":
    print("\n--- Starting DLT Data Loading Pipeline ---")

    # Configure the DLT pipeline.
    # Destination is 'athena', which reads from your .toml config.
    # Dataset name corresponds to the Athena/Glue database name.
    pipeline = dlt.pipeline(
        pipeline_name="parquet_to_athena",
        destination="athena",
        dataset_name=DB_NAME
    )

    # Get the data source. It will look for .parquet files in the current directory.
    data_source = parquet_files_source()

    # Run the pipeline
    print("Running DLT pipeline to load data...")
    load_info = pipeline.run(data_source)

    # Print the outcome
    print(load_info)
    print("--- DLT Data Loading Complete ---")