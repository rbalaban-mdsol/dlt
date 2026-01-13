# DBT Actigraph Transformations

This DBT project transforms Actigraph CentrePoint API data from the bronze layer (raw DLT ingestion) to a FHIR-compliant silver layer.

## Project Structure

```
dbt_actigraph/
├── dbt_project.yml          # Main DBT project configuration
├── profiles.yml             # Snowflake connection profiles (example)
├── packages.yml             # DBT package dependencies
├── models/
│   ├── bronze/
│   │   └── sources.yml      # Source definitions for bronze layer tables
│   └── silver/
│       ├── observation_components.sql  # Main transformation model
│       └── schema.yml       # Model documentation and tests
└── macros/
    └── deterministic_uuid_generator.sql  # UUID generation macro
```

## Setup

### Prerequisites

1. Python 3.8+ with dbt-snowflake installed
2. Snowflake credentials with access to `DATA_CONNECT_SANDBOX_DB.SENSORCLOUD`
3. Bronze layer data already ingested via the DLT pipeline

### Installation

1. Install DBT and Snowflake adapter:
```bash
pip install dbt-core dbt-snowflake
```

2. Install DBT packages:
```bash
cd dbt_actigraph
dbt deps
```

3. Configure your Snowflake connection:
   - Copy `profiles.yml` to `~/.dbt/profiles.yml`
   - Set environment variables or update with your credentials:
     - `SNOWFLAKE_ACCOUNT`
     - `SNOWFLAKE_USER`
     - `SNOWFLAKE_PASSWORD`
     - `SNOWFLAKE_ROLE`
     - `SNOWFLAKE_WAREHOUSE`

## Data Flow

### Bronze Layer (Source)
- **Table**: `DATA_CONNECT_SANDBOX_DB.SENSORCLOUD.daily_statistics`
- **Source**: Actigraph CentrePoint API via DLT pipeline
- **Format**: Iceberg table with raw API data + DLT metadata

### Silver Layer (Destination)
- **Table**: `DATA_CONNECT_SANDBOX_DB.SENSORCLOUD.observation_components`
- **Format**: Iceberg table with FHIR-compliant observation components
- **Transformations**:
  - Maps bronze fields to FHIR observation structure
  - Generates deterministic UUIDs for observation IDs
  - Creates FHIR-compliant subject, device, code, and category objects
  - Adds transformation metadata

## Usage

### Running the Transformation

1. Test the source connection:
```bash
dbt debug
```

2. Run the transformation:
```bash
dbt run
```

3. Run specific model:
```bash
dbt run --select observation_components
```

4. Run tests:
```bash
dbt test
```

5. Generate documentation:
```bash
dbt docs generate
dbt docs serve
```

### Development Workflow

1. Make changes to models in `models/silver/`
2. Test locally:
```bash
dbt run --select observation_components
dbt test --select observation_components
```
3. Review output in Snowflake
4. Commit changes

## Important Notes

### Lookup Table Integration

⚠️ **Action Required**: The current transformation includes placeholder logic for mapping study_id, subject_id, and device information to platform UUIDs.

You need to update the `lookup_data` CTE in `models/silver/observation_components.sql` with your actual lookup table joins:

```sql
lookup_data AS (
    SELECT
        bronze_data.study_id,
        bronze_data.subject_id,
        bronze_data.site_id,
        patient_lookup.platform_uuid AS platform_patient_uuid,
        device_lookup.device_id AS device_id,
        study_lookup.environment_uuid AS platform_study_environment_uuid
    FROM bronze_data
    LEFT JOIN your_patient_lookup_table AS patient_lookup 
        ON bronze_data.subject_id = patient_lookup.subject_id
    LEFT JOIN your_device_lookup_table AS device_lookup 
        ON bronze_data.activity_monitor_serials = device_lookup.serial_number
    LEFT JOIN your_study_lookup_table AS study_lookup 
        ON bronze_data.study_id = study_lookup.study_id
)
```

### Incremental Processing

The current model is configured as a full table refresh. To implement incremental processing:

1. Update the model config:
```sql
{{
    config(
        materialized='incremental',
        unique_key='observation_id'
    )
}}
```

2. Add incremental logic:
```sql
WHERE bronze.event_time > (SELECT MAX(effective_datetime) FROM {{ this }})
```

## Model Configuration

### observation_components

- **Materialization**: Table
- **Table Type**: Iceberg
- **External Volume**: SENSORCLOUD_LAKEHOUSE_VOLUME_SANDBOX
- **Base Location**: observation_components/
- **Schema**: DATA_CONNECT_SANDBOX_DB.SENSORCLOUD

## Testing

The model includes several built-in tests:
- Not null constraints on key fields
- Unique constraint on observation_id
- Accepted values for FHIR status field

Run all tests:
```bash
dbt test
```

## Troubleshooting

### Connection Issues
- Verify environment variables are set correctly
- Check Snowflake role has necessary permissions
- Ensure warehouse is running

### Data Issues
- Verify bronze layer data exists: `SELECT * FROM DATA_CONNECT_SANDBOX_DB.SENSORCLOUD.daily_statistics LIMIT 10`
- Check DLT pipeline has successfully loaded data
- Review date parsing logic if event_time is NULL

### Performance
- Consider partitioning on effective_datetime
- Add indexes if query performance is slow
- Use incremental materialization for large datasets

## Next Steps

1. Update lookup table logic with actual table joins
2. Configure incremental loading if needed
3. Add additional FHIR components (component_id, value fields)
4. Set up DBT Cloud or scheduled runs
5. Implement data quality checks and alerts
