{% macro deterministic_uuid_generator(platform_patient_uuid, device_id, event_time, source_type) %}
    -- Generate a deterministic UUID from the input parameters
    -- This macro replicates the DETERMINISTIC_UUID_GENERATOR function
    MD5(CONCAT(
        COALESCE(CAST({{ platform_patient_uuid }} AS STRING), ''),
        '|',
        COALESCE(CAST({{ device_id }} AS STRING), ''),
        '|',
        COALESCE(CAST({{ event_time }} AS STRING), ''),
        '|',
        COALESCE({{ source_type }}, '')
    ))
{% endmacro %}
