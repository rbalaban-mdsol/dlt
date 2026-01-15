"""
Daily Statistics S3 to Snowflake Pipeline

This pipeline loads daily statistics data from S3 JSONL files 
and loads them into Snowflake Iceberg tables.
"""

import dlt
from s3_actigraph_source import s3_actigraph_source


def load_daily_statistics():
    """
    Load daily statistics data from S3 to Snowflake.
    
    The S3 path and credentials should be configured in .dlt/secrets.toml:
    
    [sources.s3_actigraph]
    bucket_url = "s3://sensorcloud-pollers-sandbox/test/actigraph/daily_statistics/"
    aws_access_key_id = "YOUR_ACCESS_KEY"
    aws_secret_access_key = "YOUR_SECRET_KEY"
    aws_session_token = "YOUR_SESSION_TOKEN"  # Optional
    
    [destination.snowflake.credentials]
    database = "data_connect_sandbox_db"
    password = "YOUR_PASSWORD"
    username = "YOUR_USERNAME"
    host = "YOUR_ACCOUNT.snowflakecomputing.com"
    warehouse = "YOUR_WAREHOUSE"
    role = "YOUR_ROLE"
    """
    
    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="s3_daily_statistics_to_snowflake",
        destination="snowflake",
        dataset_name="sensorcloud",
    )
    
    # Load daily statistics data
    # Pass data_type="daily_statistics" to load JSONL files
    load_info = pipeline.run(
        s3_actigraph_source(data_type="daily_statistics")
    )
    
    print(load_info)
    return load_info


if __name__ == "__main__":
    load_daily_statistics()
