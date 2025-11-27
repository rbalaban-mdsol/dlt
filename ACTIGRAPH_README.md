# DLT Ingestion Pipeline for Actigraph CentrePoint API

This project provides a data ingestion pipeline for the Actigraph CentrePoint API using [dlt (data load tool)](https://dlthub.com/). It implements OAuth2 client credentials authentication following DLT best practices and loads daily statistics data into AWS Athena (non-Iceberg S3 tables).

## Features

- ✅ **Modular OAuth2 Authentication**: Implements client credentials flow with automatic token management
- ✅ **DLT Best Practices**: Uses `@dlt.source` decorator and proper configuration management
- ✅ **Environment-Based Credentials**: Secure credential management via environment variables
- ✅ **AWS Athena Destination**: Loads data into S3-backed Athena tables
- ✅ **Configurable Date Ranges**: Flexible querying of daily statistics

## Project Structure

```
dlt-ingestion/
├── actigraph_source.py      # Main DLT source with OAuth2 authentication
├── actigraph_pipeline.py    # Pipeline execution script
├── .dlt/
│   ├── config.toml          # Non-sensitive configuration
│   └── secrets.toml         # Sensitive credentials (not committed)
├── pyproject.toml           # Python dependencies
└── README.md                # This file
```

## Prerequisites

- Python 3.12+
- UV package manager (recommended) or pip
- AWS credentials with Athena access
- Actigraph CentrePoint API credentials (`client_id` and `client_secret`)

## Installation

### Using UV (Recommended)

```bash
# Install UV if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync
```

### Using pip

```bash
pip install -e .
```

## Configuration

### 1. Set Environment Variables

The pipeline requires Actigraph API credentials to be set as environment variables:

```bash
export CENTERPOINT_USERNAME="your_client_id"
export CENTERPOINT_PASSWORD="your_client_secret"
```

### 2. Configure Study Parameters

Edit `.dlt/config.toml` to set your study parameters:

```toml
[sources.actigraph]
study_id = 12345              # Your CentrePoint Study ID
subject_id = 67890            # Your CentrePoint Subject ID
from_date = "2024-01-01"      # Start date (ISO8601 format)
to_date = "2024-12-31"        # End date (ISO8601 format)

# Optional: Specify daily statistics settings
# daily_statistics_setting_id = "ad1fda37-239d-48ad-581b-08d8556c9944"
```

### 3. Verify AWS Credentials

The pipeline uses AWS credentials configured in `.dlt/secrets.toml`. Ensure your AWS credentials have permissions for:
- S3 bucket access
- Athena query execution
- Glue catalog access

## Usage

### Run the Pipeline

```bash
# Using UV
uv run actigraph_pipeline.py

# Using python directly
python actigraph_pipeline.py
```

### Programmatic Usage

You can also use the source in your own scripts:

```python
import dlt
from actigraph_source import actigraph_source

# Create pipeline
pipeline = dlt.pipeline(
    pipeline_name="actigraph_pipeline",
    destination="athena",
    dataset_name="actigraph_data",
)

# Run with custom parameters
source = actigraph_source(
    study_id=12345,
    subject_id=67890,
    from_date="2024-01-01",
    to_date="2024-12-31",
)

load_info = pipeline.run(source)
print(load_info)
```

## How It Works

### Authentication Flow

1. **OAuth2 Client Credentials**: The pipeline uses the `OAuth2ClientCredentials` class from DLT
2. **Automatic Token Management**: Tokens are automatically requested and refreshed
3. **Secure Storage**: Credentials are read from environment variables via `dlt.secrets`

```python
def actigraph_auth() -> OAuth2ClientCredentials:
    return OAuth2ClientCredentials(
        access_token_url="https://auth.actigraphcorp.com/connect/token",
        client_id=dlt.secrets["sources.actigraph.client_id"],
        client_secret=dlt.secrets["sources.actigraph.client_secret"],
        access_token_request_data={
            "scope": "CentrePoint DataAccess Analytics DataRetrieval"
        }
    )
```

### API Request Flow

1. Authentication function creates OAuth2 credentials
2. REST API source is configured with:
   - Base URL: `https://analytics.actigraphcorp.com/`
   - Authentication: OAuth2 client credentials
   - Endpoint: `/analytics/v3/Studies/{study_id}/Subjects/{subject_id}/DailyStatistics`
3. Query parameters include date range and optional settings ID
4. Data is extracted from the `items` field in the JSON response
5. Records are loaded into Athena with `merge` write disposition (using `id` as primary key)

## API Endpoints

### Daily Statistics

**Endpoint**: `GET /analytics/v3/Studies/{studyId}/Subjects/{subjectId}/DailyStatistics`

**Query Parameters**:
- `fromDate` (required): Start date in ISO8601 format
- `toDate` (required): End date in ISO8601 format  
- `dailyStatisticsSettingId` (optional): GUID of settings used

**Response**: Returns paginated daily statistics with comprehensive activity data including:
- Epoch aggregations (wear time, sleep, activity counts)
- Cutpoint aggregations (Crouter, Freedson, Evenson, etc.)
- Step counts (MAVM, UWF)
- METs and calorie calculations

## Data Schema

The `daily_statistics` table in Athena will contain:

| Field | Type | Description |
|-------|------|-------------|
| id | Number | Daily Statistic ID (Primary Key) |
| studyId | Number | CentrePoint Study ID |
| subjectId | Number | CentrePoint Subject ID |
| siteId | Number | CentrePoint Site ID |
| date | String | Date of the daily statistic |
| epochAggregation | Object | Aggregated epoch data |
| crouterAggregations | Array | Crouter cutpoint data |
| freedsonAggregations | Array | Freedson cutpoint data |
| mavmAggregation | Object | MAVM steps data |
| ... | ... | Additional fields per API spec |

## Troubleshooting

### Authentication Errors (401)

```
Error: 401 Unauthorized
```

**Solutions**:
- Verify `CENTERPOINT_USERNAME` and `CENTERPOINT_PASSWORD` environment variables are set
- Check that credentials are valid in the Actigraph portal
- Ensure the scope includes: `CentrePoint DataAccess Analytics DataRetrieval`

### Configuration Errors

```
DictValidationException: field 'sources.actigraph.client_id' is required
```

**Solutions**:
- Ensure environment variables are exported in your shell
- Check `.dlt/secrets.toml` has the correct placeholders: `"${CENTERPOINT_USERNAME}"`
- Verify `dlt.secrets` can resolve the values

### No Data Returned

**Solutions**:
- Verify `study_id` and `subject_id` are correct
- Check date range has available data
- Inspect logs with: `export RUNTIME__LOG_LEVEL=INFO`

### AWS/Athena Errors

**Solutions**:
- Verify AWS credentials in `.dlt/secrets.toml`
- Ensure S3 bucket exists and is accessible
- Check Athena database permissions

## Advanced Configuration

### Custom Athena Settings

Edit `.dlt/secrets.toml` for Athena-specific settings:

```toml
[destination.athena]
force_iceberg = "False"  # Use standard S3 tables (non-Iceberg)
query_result_bucket = "s3://your-bucket/athena-results/"
database = "actigraph_db"
```

### Enable Detailed Logging

Set log level in `.dlt/config.toml`:

```toml
[runtime]
log_level = "INFO"  # Or "DEBUG" for verbose output
```

## API Documentation

For complete API documentation, see:
- [CentrePoint API Authorization](https://github.com/actigraph/CentrePoint3APIDocumentation/blob/main/sections/authorization.md)
- [Daily Statistics Endpoint](https://github.com/actigraph/CentrePoint3APIDocumentation/blob/main/sections/daily_statistics.md)

## License

This project is provided as-is for use with Actigraph CentrePoint API integration.

## Support

For issues or questions:
1. Check the [DLT documentation](https://dlthub.com/docs)
2. Review [Actigraph API docs](https://github.com/actigraph/CentrePoint3APIDocumentation)
3. Join the [DLT Slack community](https://dlthub.com/community)
