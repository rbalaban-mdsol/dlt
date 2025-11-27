"""
Quick start example for Actigraph CentrePoint API ingestion.

This script demonstrates a minimal setup for loading data from the Actigraph API.
"""

import os
import dlt
from actigraph_source import actigraph_source


def quick_start_example():
    """
    Quick start example that shows how to configure and run the pipeline.
    """
    
    # Step 1: Set environment variables for credentials
    # You can set these in your shell or use a .env file
    if not os.getenv("CENTERPOINT_USERNAME"):
        print("⚠️  Warning: CENTERPOINT_USERNAME environment variable not set")
        print("   Run: export CENTERPOINT_USERNAME='your_client_id'")
        return
    
    if not os.getenv("CENTERPOINT_PASSWORD"):
        print("⚠️  Warning: CENTERPOINT_PASSWORD environment variable not set")
        print("   Run: export CENTERPOINT_PASSWORD='your_client_secret'")
        return
    
    # Step 2: Configure your study parameters
    # Replace these with your actual values or use config.toml
    STUDY_ID = 2775  # Example Study ID
    SUBJECT_ID = 22518  # Example Subject ID
    FROM_DATE = "2024-01-01"
    TO_DATE = "2024-01-31"
    
    print("\n" + "="*60)
    print("Actigraph CentrePoint API - Quick Start Example")
    print("="*60)
    print(f"\nConfiguration:")
    print(f"  Study ID: {STUDY_ID}")
    print(f"  Subject ID: {SUBJECT_ID}")
    print(f"  Date Range: {FROM_DATE} to {TO_DATE}")
    print(f"  Destination: AWS Athena")
    
    # Step 3: Create the DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name="actigraph_quickstart",
        destination="athena",
        dataset_name="actigraph_data",
    )
    
    # Step 4: Create the data source
    print("\n📡 Connecting to Actigraph API...")
    source = actigraph_source(
        study_id=STUDY_ID,
        subject_id=SUBJECT_ID,
        from_date=FROM_DATE,
        to_date=TO_DATE,
    )
    
    # Step 5: Run the pipeline
    print("⚙️  Running data ingestion pipeline...")
    try:
        load_info = pipeline.run(source)
        
        # Step 6: Check results
        print("\n" + "="*60)
        print("Pipeline Results")
        print("="*60)
        print(load_info)
        
        if load_info.has_failed_jobs:
            print("\n❌ Some jobs failed!")
            for package in load_info.load_packages:
                for job in package.jobs.get('failed_jobs', []):
                    print(f"  Failed: {job.job_file_info.job_id()}")
        else:
            print("\n✅ Success! Data loaded to Athena")
            print(f"\nQuery your data with:")
            print(f"  Database: actigraph_data")
            print(f"  Table: daily_statistics")
            print(f"\nExample SQL:")
            print("  SELECT * FROM actigraph_data.daily_statistics LIMIT 10;")
        
    except Exception as e:
        print(f"\n❌ Error running pipeline: {e}")
        print("\nTroubleshooting:")
        print("1. Check your environment variables are set correctly")
        print("2. Verify your Actigraph API credentials are valid")
        print("3. Ensure AWS credentials in .dlt/secrets.toml are correct")
        print("4. Check study_id and subject_id exist in your Actigraph account")
        raise
    
    print("\n" + "="*60)


if __name__ == "__main__":
    quick_start_example()
