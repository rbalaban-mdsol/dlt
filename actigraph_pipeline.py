"""
Actigraph CentrePoint API Data Pipeline

This script runs the DLT pipeline to ingest daily statistics data from 
the Actigraph CentrePoint API into AWS Athena.
"""

import dlt
from actigraph_source import actigraph_source


def main(refresh: bool = False):
    """
    Main function to run the Actigraph data pipeline.
    
    Configuration is read from .dlt/config.toml and .dlt/secrets.toml
    
    Args:
        refresh: If True, ignores incremental state and reloads all data
    """
    print("\n--- Starting Actigraph Data Pipeline ---")
    
    # Create the DLT pipeline
    # Using Athena destination with Iceberg format for native partition support
    pipeline = dlt.pipeline(
        pipeline_name="actigraph_pipeline",
        destination="athena",
        dataset_name="actigraph_data",
    )
    
    # Load configuration from .dlt/config.toml for display
    # DLT will automatically inject these values into the source function
    study_id = dlt.config.get("sources.actigraph_source.actigraph.study_id")
    subject_id = dlt.config.get("sources.actigraph_source.actigraph.subject_id")
    from_date = dlt.config.get("sources.actigraph_source.actigraph.from_date")
    to_date = dlt.config.get("sources.actigraph_source.actigraph.to_date")
    
    print(f"Configuration:")
    print(f"  Study ID: {study_id}")
    print(f"  Subject ID: {subject_id}")
    print(f"  Date Range: {from_date} to {to_date}")
    print(f"  Refresh Mode: {refresh}")
    
    # Create the source - DLT will automatically inject config values
    # No need to pass parameters explicitly  
    source = actigraph_source()
    
    # Reset incremental state if refresh mode is enabled
    if refresh:
        print("\n⚠️  Refresh mode enabled - ignoring incremental state")
        source.daily_statistics.apply_hints(incremental=dlt.sources.incremental(cursor_path="lastEpochDateTimeUtc", initial_value="1970-01-01T00:00:00Z"))
    
    # Apply partition hints for the resource
    source.daily_statistics.apply_hints(
        write_disposition="append",
        columns={
            "study_id": {"partition": True},
            "ingestion_date": {"partition": True}
        }
    )
    
    # Run the pipeline
    print("\nRunning pipeline to load data from Actigraph API...")
    load_info = pipeline.run(source)
    
    # Print the results
    print("\n--- Pipeline Results ---")
    print(load_info)
    
    # Print table names created
    if load_info.has_failed_jobs:
        print("\n Some jobs failed!")
        for job in load_info.load_packages[0].jobs['failed_jobs']:
            print(f"Failed job: {job.job_file_info.job_id()}")
    else:
        print("\n Pipeline completed successfully!")
        print(f"\n Data loaded to S3 bronze layer: s3://msc-sandbox-iceberg/bronze/actigraph_data")
        print(f"Table created: daily_statistics")
    
    print("\n--- Actigraph Data Pipeline Complete ---")


if __name__ == "__main__":
    import sys
    
    # Check for --refresh or -r flag
    refresh = "--refresh" in sys.argv or "-r" in sys.argv
    
    main(refresh=refresh)
