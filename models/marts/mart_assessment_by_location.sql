{{
    config(
        materialized='table'
    )
}}

/*
    MART: Assessments by Location

    Shows count of assessments by country and gender.
    Use for geographic breakdown charts with gender split.

    Grain: One row per country + gender combination
*/

WITH country_data AS (
    -- Get country for each record (multiple possible field_keys)
    SELECT
        record_id,
        field_value_text AS country
    FROM {{ ref('int_form_records_flat') }}
    WHERE field_key IN ('5e30', '9e50', '2b1a', 'b690', '2853', 'c4e0')  -- Country field keys
      AND field_value_text IS NOT NULL
      AND field_value_text != ''
),

gender_data AS (
    -- Get gender for each record (multiple possible field_keys)
    SELECT
        record_id,
        field_value_text AS gender
    FROM {{ ref('int_form_records_flat') }}
    WHERE field_key IN ('763d', '64fc', '1a31', '3cb0', '2960')  -- Gender field keys
      AND field_value_text IS NOT NULL
),

record_base AS (
    SELECT DISTINCT
        record_id,
        fulcrum_status,
        created_at
    FROM {{ ref('int_form_records_flat') }}
),

records_enriched AS (
    SELECT
        rb.record_id,
        rb.fulcrum_status,
        rb.created_at,
        cd.country,
        gd.gender
    FROM record_base rb
    LEFT JOIN country_data cd ON rb.record_id = cd.record_id
    LEFT JOIN gender_data gd ON rb.record_id = gd.record_id
)

SELECT
    COALESCE(country, 'Unknown') AS country,
    COALESCE(gender, 'Not specified') AS gender,
    COUNT(DISTINCT record_id) AS total_assessments,
    COUNT(DISTINCT CASE WHEN fulcrum_status = 'registered' THEN record_id END) AS registered,
    COUNT(DISTINCT CASE WHEN fulcrum_status = 'enrolled' THEN record_id END) AS enrolled,
    COUNT(DISTINCT CASE WHEN fulcrum_status = 'passed' THEN record_id END) AS passed,
    COUNT(DISTINCT CASE WHEN fulcrum_status = 'selection_go' THEN record_id END) AS selection_go,
    MIN(created_at) AS first_assessment,
    MAX(created_at) AS last_assessment
FROM records_enriched
GROUP BY country, gender
ORDER BY total_assessments DESC
