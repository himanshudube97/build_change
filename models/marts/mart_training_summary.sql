{{
    config(
        materialized='table'
    )
}}

/*
    MART: Training Summary

    Shows training types and participation.
    Use for bar charts showing training breakdown.

    Grain: One row per training_type + training_subtype combination
*/

WITH training_type AS (
    SELECT
        record_id,
        field_value_text AS training_type
    FROM {{ ref('int_form_records_flat') }}
    WHERE field_key IN ('b970', '2870')  -- Training type field keys
      AND field_value_text IS NOT NULL
      AND field_value_text != ''
),

training_subtype AS (
    SELECT
        record_id,
        field_value_text AS training_subtype
    FROM {{ ref('int_form_records_flat') }}
    WHERE field_key IN ('294a', '36b0')  -- Training sub-type field keys
      AND field_value_text IS NOT NULL
      AND field_value_text != ''
),

training_location AS (
    SELECT
        record_id,
        field_value_text AS training_location_type
    FROM {{ ref('int_form_records_flat') }}
    WHERE field_key IN ('8be0', '9e30')  -- Type of training location field keys
      AND field_value_text IS NOT NULL
      AND field_value_text != ''
),

record_base AS (
    SELECT DISTINCT
        record_id,
        fulcrum_status,
        created_at
    FROM {{ ref('int_form_records_flat') }}
),

training_enriched AS (
    SELECT
        rb.record_id,
        rb.fulcrum_status,
        rb.created_at,
        tt.training_type,
        ts.training_subtype,
        tl.training_location_type
    FROM record_base rb
    LEFT JOIN training_type tt ON rb.record_id = tt.record_id
    LEFT JOIN training_subtype ts ON rb.record_id = ts.record_id
    LEFT JOIN training_location tl ON rb.record_id = tl.record_id
    WHERE tt.training_type IS NOT NULL
       OR ts.training_subtype IS NOT NULL
       OR tl.training_location_type IS NOT NULL
)

SELECT
    COALESCE(training_type, 'Not specified') AS training_type,
    COALESCE(training_subtype, 'Not specified') AS training_subtype,
    COALESCE(training_location_type, 'Not specified') AS training_location_type,
    COUNT(DISTINCT record_id) AS total_participants,
    COUNT(DISTINCT CASE WHEN fulcrum_status = 'passed' THEN record_id END) AS passed,
    COUNT(DISTINCT CASE WHEN fulcrum_status = 'not_passed' THEN record_id END) AS not_passed,
    COUNT(DISTINCT CASE WHEN fulcrum_status IN ('internal_training', 'external_training') THEN record_id END) AS in_training,
    MIN(created_at) AS first_training,
    MAX(created_at) AS last_training
FROM training_enriched
GROUP BY training_type, training_subtype, training_location_type
ORDER BY total_participants DESC
