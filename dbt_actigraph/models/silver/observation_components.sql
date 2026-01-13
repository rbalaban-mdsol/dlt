{{
    config(
        alias='silver_observation_components_staging',
        materialized='view',
        snowflake_warehouse='SENSORCLOUD_DATACONNECT_SANDBOX_VW',
        post_hook="""
        CREATE ICEBERG TABLE IF NOT EXISTS DATA_CONNECT_SANDBOX_DB.SENSORCLOUD.silver_observation_components (
            observation_id STRING,
            effective_datetime TIMESTAMP,
            effective_period_start TIMESTAMP,
            effective_period_end TIMESTAMP,
            subject STRING,
            device STRING,
            study_environment STRING,
            status STRING,
            code STRING,
            category STRING,
            partOf STRING,
            derivedFrom STRING,
            body_site STRING,
            interpretation STRING,
            metadata STRING,
            bronze_id BIGINT,
            study_id BIGINT,
            subject_id BIGINT,
            site_id BIGINT,
            _dlt_load_id STRING,
            _dlt_id STRING
        )
        CATALOG = 'SNOWFLAKE'
        EXTERNAL_VOLUME = 'SENSORCLOUD_LAKEHOUSE_VOLUME_SANDBOX'
        BASE_LOCATION = 'silver_observation_components/';
        
        INSERT OVERWRITE INTO DATA_CONNECT_SANDBOX_DB.SENSORCLOUD.silver_observation_components
        SELECT * FROM {{ this }};
        """
    )
}}

/*
Silver Layer Transformation: Actigraph Daily Statistics to FHIR Observation Components

This model transforms bronze layer Actigraph daily statistics data into 
FHIR-compliant observation components for the silver layer.

Note: This transformation assumes a lookup table exists to map study_id, subject_id, 
and device information to platform UUIDs. You'll need to adjust the join logic 
based on your actual lookup table structure.
*/

WITH bronze_data AS (
    SELECT 
        *,
        -- Parse the date field to create event_time
        TRY_TO_TIMESTAMP(date, 'YYYY-MM-DD') AS event_time
    FROM {{ source('actigraph_bronze', 'daily_statistics') }}
),

-- TODO: Replace this CTE with your actual lookup table logic
-- This is a placeholder structure showing what fields are needed
lookup_data AS (
    SELECT
        bronze_data.study_id,
        bronze_data.subject_id,
        bronze_data.site_id,
        -- Placeholder values - replace with actual lookup table joins
        'PLACEHOLDER_PATIENT_UUID' AS platform_patient_uuid,
        'PLACEHOLDER_DEVICE_ID' AS device_id,
        'PLACEHOLDER_STUDY_ENV_UUID' AS platform_study_environment_uuid
    FROM bronze_data
    -- TODO: Add actual JOIN logic to your lookup tables here
    -- Example:
    -- LEFT JOIN your_patient_lookup_table ON ...
    -- LEFT JOIN your_device_lookup_table ON ...
),

final AS (
    SELECT
        -- Generate observation ID using deterministic UUID
        {{ deterministic_uuid_generator(
            'lookup.platform_patient_uuid', 
            'lookup.device_id', 
            'CAST(bronze.event_time AS STRING)', 
            "'ACTIGRAPH_DAILY_STATS'"
        ) }} AS observation_id,
        
        -- Temporal fields
        bronze.event_time AS effective_datetime,
        CAST(
            NULLIF(TO_VARCHAR(bronze.event_time, 'YYYY-MM-DD') || ' 00:00:00.000', '') 
            AS TIMESTAMP
        ) AS effective_period_start,
        CAST(
            NULLIF(TO_VARCHAR(bronze.event_time, 'YYYY-MM-DD') || ' 23:59:59.999', '') 
            AS TIMESTAMP
        ) AS effective_period_end,
        
        -- FHIR subject reference (convert to JSON string)
        TO_JSON(OBJECT_CONSTRUCT(
            'reference', lookup.platform_patient_uuid,
            'type', 'Patient'
        )) AS subject,
        
        -- FHIR device reference (convert to JSON string)
        TO_JSON(OBJECT_CONSTRUCT(
            'reference', lookup.device_id,
            'type', 'Device'
        )) AS device,
        
        -- Study environment reference (convert to JSON string)
        TO_JSON(OBJECT_CONSTRUCT(
            'reference', lookup.platform_study_environment_uuid,
            'type', 'Platform Study Environment UUID'
        )) AS study_environment,
        
        -- FHIR observation status
        'final' AS status,
        
        -- FHIR code (observation type) (convert to JSON string)
        TO_JSON(OBJECT_CONSTRUCT(
            'coding', ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'code', 'ACTIGRAPH_DAILY_STATS',
                    'display', 'Actigraph Daily Statistics',
                    'system', 'http://mdsol.com'
                )
            )
        )) AS code,
        
        -- FHIR category (convert to JSON string)
        TO_JSON(OBJECT_CONSTRUCT(
            'system', 'http://terminology.hl7.org/CodeSystem/observation-category',
            'code', 'device',
            'display', 'Device'
        )) AS category,
        
        -- FHIR references (placeholders) (convert to JSON string)
        TO_JSON(ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT(
                'reference', CAST(NULL AS STRING),
                'type', CAST(NULL AS STRING)
            )
        )) AS partOf,
        
        TO_JSON(ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT(
                'reference', CAST(NULL AS STRING),
                'type', CAST(NULL AS STRING)
            )
        )) AS derivedFrom,
        
        -- Body site (placeholder) (convert to JSON string)
        TO_JSON(OBJECT_CONSTRUCT(
            'coding', ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'system', CAST(NULL AS STRING),
                    'code', CAST(NULL AS STRING),
                    'display', CAST(NULL AS STRING)
                )
            )
        )) AS body_site,
        
        -- Interpretation (placeholder) (convert to JSON string)
        TO_JSON(ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT(
                'coding', ARRAY_CONSTRUCT(
                    OBJECT_CONSTRUCT(
                        'system', CAST(NULL AS STRING),
                        'code', CAST(NULL AS STRING),
                        'display', CAST(NULL AS STRING)
                    )
                )
            )
        )) AS interpretation,
        
        -- Metadata for tracking (convert to JSON string)
        TO_JSON(OBJECT_CONSTRUCT(
            'processed_at', CURRENT_TIMESTAMP(),
            'source_file', bronze.ingestion_date,
            'transformation_job', 'DBT_ACTIGRAPH_SILVER',
            'transformation_description', 'FHIR Silver Observation Transformation'
        )) AS metadata,
        
        -- Original bronze data for reference
        bronze.id AS bronze_id,
        bronze.study_id,
        bronze.subject_id,
        bronze.site_id,
        bronze._dlt_load_id,
        bronze._dlt_id

    FROM bronze_data AS bronze
    CROSS JOIN lookup_data AS lookup
)

SELECT * FROM final
