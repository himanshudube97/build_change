{{
    config(
        materialized='table'
    )
}}

/*
    FLAT TABLE: Form Records with ALL JSON fields flattened

    - data: Unpivoted to rows (one row per field_key per record)
    - cnf: Flattened to columns (arrays as comma-separated)
    - fulcrum: Flattened to columns
    - project: Flattened to columns (arrays as comma-separated)
    - metadata: Flattened to columns

    Grain: One row per FIELD VALUE per form record
    (A record with 10 fields = 10 rows)
*/

WITH form_records_base AS (
    SELECT
        -- ========== RECORD IDENTIFIERS ==========
        _id AS record_id,
        uid AS record_uid,
        form_id,
        user_id,
        building_id,

        -- ========== RECORD STATE ==========
        state AS record_state,
        consent,
        is_edited,

        -- ========== TIMESTAMPS ==========
        created_at,
        updated_at,

        -- ========== CNF (Configuration) - Flattened ==========
        cnf->>'display' AS cnf_display,
        -- Arrays as comma-separated strings
        (
            SELECT string_agg(elem::text, ', ')
            FROM jsonb_array_elements_text(cnf->'identities') AS elem
        ) AS cnf_identities,
        (
            SELECT string_agg(elem::text, ', ')
            FROM jsonb_array_elements_text(cnf->'identifiers') AS elem
        ) AS cnf_identifiers,

        -- ========== FULCRUM (Sync Metadata) - Flattened ==========
        fulcrum->>'status' AS fulcrum_status,
        (fulcrum->>'latitude')::numeric AS fulcrum_latitude,
        (fulcrum->>'longitude')::numeric AS fulcrum_longitude,
        fulcrum->>'record_id' AS fulcrum_record_id,
        (fulcrum->>'version')::int AS fulcrum_version,
        fulcrum->>'assignee_name' AS fulcrum_assignee_name,
        fulcrum->>'current_tranche' AS fulcrum_current_tranche,
        fulcrum->>'record_updated_at' AS fulcrum_record_updated_at,

        -- ========== PROJECT - Flattened ==========
        project->>'id' AS project_ref_id,
        (
            SELECT string_agg(elem::text, ', ')
            FROM jsonb_array_elements_text(project->'assignees') AS elem
        ) AS project_assignees,

        -- ========== METADATA - Flattened ==========
        -- Mostly NULL, but extract common keys if present
        metadata->>'source' AS metadata_source,
        metadata->>'version' AS metadata_version,

        -- ========== ZIPPED FILE - Flattened ==========
        zipped_file->>'url' AS zipped_file_url,
        zipped_file->>'name' AS zipped_file_name,

        -- ========== AIRBYTE METADATA ==========
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _ab_cdc_cursor,
        _ab_cdc_updated_at,
        _ab_cdc_deleted_at,

        -- Keep data for unpivoting
        data

    FROM {{ source('staging', 'form_records') }}
    WHERE data IS NOT NULL
),

-- Unpivot the dynamic 'data' column to rows
data_unpivoted AS (
    SELECT
        frb.*,
        kv.key AS field_key,
        kv.value AS field_value_json,
        -- Extract as text for simple values
        CASE
            WHEN jsonb_typeof(kv.value) = 'string' THEN kv.value #>> '{}'
            WHEN jsonb_typeof(kv.value) = 'number' THEN kv.value::text
            WHEN jsonb_typeof(kv.value) = 'boolean' THEN kv.value::text
            WHEN jsonb_typeof(kv.value) = 'null' THEN NULL
            ELSE kv.value::text  -- arrays/objects as JSON string
        END AS field_value_text,
        -- Type of the value
        jsonb_typeof(kv.value) AS field_value_type
    FROM form_records_base frb,
    LATERAL jsonb_each(frb.data) AS kv(key, value)
)

SELECT
    -- ========== RECORD IDENTIFIERS ==========
    record_id,
    record_uid,
    form_id,
    user_id,
    building_id,

    -- ========== RECORD STATE ==========
    record_state,
    consent,
    is_edited,

    -- ========== TIMESTAMPS ==========
    created_at,
    updated_at,

    -- ========== CNF (Flattened) ==========
    cnf_display,
    cnf_identities,
    cnf_identifiers,

    -- ========== FULCRUM (Flattened) ==========
    fulcrum_status,
    fulcrum_latitude,
    fulcrum_longitude,
    fulcrum_record_id,
    fulcrum_version,
    fulcrum_assignee_name,
    fulcrum_current_tranche,
    fulcrum_record_updated_at,

    -- ========== PROJECT (Flattened) ==========
    project_ref_id,
    project_assignees,

    -- ========== METADATA (Flattened) ==========
    metadata_source,
    metadata_version,

    -- ========== ZIPPED FILE (Flattened) ==========
    zipped_file_url,
    zipped_file_name,

    -- ========== FIELD DATA (Unpivoted) ==========
    field_key,
    field_value_text,
    field_value_type,
    field_value_json AS _raw_field_value,

    -- ========== AIRBYTE METADATA ==========
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _ab_cdc_cursor,
    _ab_cdc_updated_at,
    _ab_cdc_deleted_at

FROM data_unpivoted
ORDER BY record_id, field_key
