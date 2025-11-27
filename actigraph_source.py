"""
Actigraph CentrePoint API DLT Source

This module provides a DLT source for ingesting data from the Actigraph CentrePoint API.
It implements OAuth2 client credentials authentication following DLT best practices.
"""

import dlt
from dlt.sources.helpers.rest_client.auth import AuthConfigBase
from dlt.sources.rest_api import rest_api_source
from dlt.common.typing import TSecretStrValue
from dlt.common.configuration import configspec
from requests.auth import AuthBase
from requests import PreparedRequest, Session
import requests


@configspec
class ActigraphOAuth2(AuthConfigBase, AuthBase):
    """Custom OAuth2 authentication for Actigraph CentrePoint API."""
    
    access_token_url: str = None
    client_id: TSecretStrValue = None
    client_secret: TSecretStrValue = None
    scope: str = None
    
    def __init__(
        self,
        access_token_url: str = None,
        client_id: TSecretStrValue = None,
        client_secret: TSecretStrValue = None,
        scope: str = None,
    ):
        super().__init__()
        self.access_token_url = access_token_url or self.access_token_url
        self.client_id = client_id or self.client_id
        self.client_secret = client_secret or self.client_secret
        self.scope = scope or self.scope
        self.access_token: str = None
        self.session = Session()
    
    def obtain_token(self) -> str:
        """Obtain OAuth2 access token from the auth server."""
        data = {
            "grant_type": "client_credentials",
            "client_id": str(self.client_id),
            "client_secret": str(self.client_secret),
            "scope": self.scope,
        }
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        response = self.session.post(
            self.access_token_url,
            data=data,
            headers=headers
        )
        response.raise_for_status()
        
        token_data = response.json()
        self.access_token = token_data["access_token"]
        return self.access_token
    
    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        """Add authorization header to the request."""
        if not self.access_token:
            self.obtain_token()
        
        request.headers["Authorization"] = f"Bearer {self.access_token}"
        return request


def actigraph_auth() -> ActigraphOAuth2:
    """
    Creates and returns an OAuth2 authentication object for Actigraph API.
    
    This function retrieves credentials from environment variables and configures OAuth2 client
    credentials flow with the appropriate scope for CentrePoint API access.
    
    Returns:
        ActigraphOAuth2: Configured authentication object
    """
    import os
    return ActigraphOAuth2(
        access_token_url="https://auth.actigraphcorp.com/connect/token",
        client_id=os.getenv("CENTERPOINT_USERNAME"),
        client_secret=os.getenv("CENTERPOINT_PASSWORD"),
        scope="CentrePoint DataAccess Analytics DataRetrieval",
    )


@dlt.source(name="actigraph")
def actigraph_source(
    study_id: int = dlt.config.value,
    subject_id: int = dlt.config.value,
    from_date: str = dlt.config.value,
    to_date: str = dlt.config.value,
    daily_statistics_setting_id: str = None,
):
    """
    DLT source for Actigraph CentrePoint API.
    
    This source fetches daily statistics data from the Actigraph CentrePoint API
    using OAuth2 client credentials authentication with incremental loading.
    
    Args:
        study_id: CentrePoint Study ID
        subject_id: CentrePoint Subject ID
        from_date: Starting date for the daily statistics query (ISO8601 format)
        to_date: Ending date for the daily statistics query (ISO8601 format)
        daily_statistics_setting_id: Optional GUID of the settings used to create the daily statistics
    
    Yields:
        DLT resources containing daily statistics data with partitioning columns
    """
    
    # Build query parameters
    params = {
        "fromDate": from_date,
        "toDate": to_date,
    }
    
    if daily_statistics_setting_id:
        params["dailyStatisticsSettingId"] = daily_statistics_setting_id
    
    # Configure the REST API source
    config = {
        "client": {
            "base_url": "https://api.actigraphcorp.com/",
            "auth": actigraph_auth(),
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {
                "params": params,
            }
        },
        "resources": [
            {
                "name": "daily_statistics",
                "endpoint": {
                    "path": f"analytics/v3/Studies/{study_id}/Subjects/{subject_id}/DailyStatistics",
                    "data_selector": "items",
                    "incremental": {
                        "cursor_path": "lastEpochDateTimeUtc",
                        "initial_value": "1970-01-01T00:00:00Z",  # ISO datetime string to match API data type
                    }
                },
                "max_table_nesting": 0,  # Keep all nested data in main table
            }
        ],
    }
    
    # Get the REST API source
    source = rest_api_source(config)
    
    # Function to add partition columns to each record
    def add_partition_columns(item):
        """
        Add partition columns to each record.
        
        This function adds:
        - study_id: For partitioning by study
        - ingestion_date: For partitioning by ingestion date
        """
        from datetime import datetime
        
        item["study_id"] = study_id
        item["ingestion_date"] = datetime.utcnow().date().isoformat()
        return item
    
    # Apply the mapping function
    resource = source.daily_statistics.add_map(add_partition_columns)
    
    return resource


def load_daily_statistics(
    study_id: int,
    subject_id: int,
    from_date: str,
    to_date: str,
    daily_statistics_setting_id: str = None,
    destination: str = "athena",
    dataset_name: str = "actigraph_data",
) -> None:
    """
    Load daily statistics from Actigraph CentrePoint API to the destination.
    
    Args:
        study_id: CentrePoint Study ID
        subject_id: CentrePoint Subject ID
        from_date: Starting date for the daily statistics query (ISO8601 format, e.g., "2024-01-01")
        to_date: Ending date for the daily statistics query (ISO8601 format, e.g., "2024-12-31")
        daily_statistics_setting_id: Optional GUID of the settings used to create the daily statistics
        destination: DLT destination (default: "athena")
        dataset_name: Name of the dataset in the destination (default: "actigraph_data")
    
    Returns:
        None
    """
    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="actigraph_pipeline",
        destination=destination,
        dataset_name=dataset_name,
    )
    
    # Run the pipeline
    load_info = pipeline.run(
        actigraph_source(
            study_id=study_id,
            subject_id=subject_id,
            from_date=from_date,
            to_date=to_date,
            daily_statistics_setting_id=daily_statistics_setting_id,
        )
    )
    
    print(load_info)


if __name__ == "__main__":
    # Example usage - these values should come from config
    load_daily_statistics(
        study_id=dlt.config["sources.actigraph.study_id"],
        subject_id=dlt.config["sources.actigraph.subject_id"],
        from_date=dlt.config["sources.actigraph.from_date"],
        to_date=dlt.config["sources.actigraph.to_date"],
    )
