from dotenv import load_dotenv

import boto3
import pandas as pd
import numpy as np
import time
import os

load_dotenv()

S3_STAGING_DIR = "s3://msc-sandbox-iceberg/athena-result/"
S3_TABLE_LOCATION = "s3://sensorcloud-lakehouse-sandbox/iceberg_catalog/sandbox_static.db" # S3 location for the Iceberg table data

# --- Database and Table Configuration ---
DB_NAME = "sandbox_static"
TABLE_NAME = "dlt_demo"

session = boto3.Session(region_name=os.getenv("AWS_REGION"))
glue_client = session.client("glue")
athena_client = session.client("athena")


def create_database_and_table():
    """
    Checks for and creates the Glue database and the Athena Iceberg table if they don't exist.
    """
    try:
        glue_client.get_database(Name=DB_NAME)
        print(f"Database '{DB_NAME}' already exists.")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"Database '{DB_NAME}' not found. Creating it...")
        glue_client.create_database(
            DatabaseInput={"Name": DB_NAME, "Description": "Sandbox for DLT demos"}
        )
        print(f"Database '{DB_NAME}' created successfully.")

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {DB_NAME}.{TABLE_NAME} (
        x int,
        y int,
        z int,
        timestamp timestamp
    )
    LOCATION '{S3_TABLE_LOCATION}'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG'
    )
    """

    print(f"\nExecuting CREATE TABLE query for '{DB_NAME}.{TABLE_NAME}'...")
    print(create_table_query)

    try:
        response = athena_client.start_query_execution(
            QueryString=create_table_query,
            QueryExecutionContext={"Database": DB_NAME},
            ResultConfiguration={"OutputLocation": S3_STAGING_DIR},
        )
        query_execution_id = response["QueryExecutionId"]

        state = "RUNNING"
        while state in ["RUNNING", "QUEUED"]:
            result = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            state = result["QueryExecution"]["Status"]["State"]
            if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
            print("Query is running...")
            time.sleep(5)

        if state == "SUCCEEDED":
            print(f"Table '{DB_NAME}.{TABLE_NAME}' created or already exists. Query SUCCEEDED.")
        else:
            error_message = result["QueryExecution"]["Status"].get("StateChangeReason", "No reason provided.")
            print(f"Query FAILED. Reason: {error_message}")

    except Exception as e:
        print(f"An error occurred while executing the Athena query: {e}")

    print(f"--- Database and Table Setup Complete ---")


def generate_parquet_file(num_rows: int, filename: str = "random_data.parquet"):
    """
    Generates a Parquet file with random data for x, y, z, and timestamp columns.

    Args:
        num_rows (int): The number of rows of data to generate.
        filename (str): The name of the output parquet file. Defaults to "random_data.parquet".
    """
    print(f"\n--- Generating Parquet File ---")
    if num_rows <= 0:
        print("Number of rows must be positive.")
        return

    print(f"Generating {num_rows} rows of random data...")

    # Generate random data efficiently using NumPy
    data = {
        'x': np.random.randint(0, 1000, size=num_rows, dtype=np.int32),
        'y': np.random.randint(0, 1000, size=num_rows, dtype=np.int32),
        'z': np.random.randint(0, 1000, size=num_rows, dtype=np.int32),
        # Generate random timestamps within the last 5 years
        'timestamp': pd.to_datetime(
            np.random.randint(
                int(time.time()) - (86400 * 365 * 5),
                int(time.time()),
                size=num_rows
            ),
            unit='s'
        )
    }

    df = pd.DataFrame(data)

    df['timestamp'] = df['timestamp'].astype('datetime64[ns]')

    output_path = os.path.join(os.getcwd(), filename)
    try:
        df.to_parquet(output_path, engine='pyarrow')
        print(f"Successfully generated and saved Parquet file to: {output_path}")
        print("\nDataFrame sample:")
        print(df.head())
    except Exception as e:
        print(f"Failed to write Parquet file. Error: {e}")


if __name__ == "__main__":
    create_database_and_table()
    generate_parquet_file(num_rows=1024)