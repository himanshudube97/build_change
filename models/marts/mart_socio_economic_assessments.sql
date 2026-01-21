{{
    config(
        materialized='table'
    )
}}

/*
    MART: Socio Economic Info - House Assessments

    Shows how many houses were assessed, broken down by:
    - Status (fulcrum_status = data movement)
    - Location (country, branch)
    - Gender

    Use for: Stacked bar charts, pivot tables, filtered dashboards

    Grain: One row per record (house/assessment)
*/

-- Get country for each record
WITH country_data AS (
    SELECT DISTINCT
        record_id,
        FIRST_VALUE(field_value_text) OVER (
            PARTITION BY record_id
            ORDER BY CASE WHEN field_value_text IS NOT NULL AND field_value_text <> '' THEN 0 ELSE 1 END
        ) AS country
    FROM {{ ref('int_form_records_flat') }}
    WHERE field_key IN ('5e30', '9e50', '2b1a', 'b690', '2853', 'c4e0')
),

-- Get gender for each record
gender_data AS (
    SELECT DISTINCT
        record_id,
        FIRST_VALUE(field_value_text) OVER (
            PARTITION BY record_id
            ORDER BY CASE WHEN field_value_text IS NOT NULL AND field_value_text <> '' THEN 0 ELSE 1 END
        ) AS gender
    FROM {{ ref('int_form_records_flat') }}
    WHERE field_key IN ('763d', '64fc', '1a31', '3cb0', '2960')
),

-- Get branch location for each record
branch_data AS (
    SELECT DISTINCT
        record_id,
        field_value_text AS branch_location
    FROM {{ ref('int_form_records_flat') }}
    WHERE field_key = 'branchLocation'
      AND field_value_text IS NOT NULL
      AND field_value_text <> ''
),

-- Get base record info (one row per record)
record_base AS (
    SELECT DISTINCT
        record_id,
        record_uid,
        form_id,
        building_id,
        record_state,
        fulcrum_status,
        fulcrum_latitude,
        fulcrum_longitude,
        project_ref_id,
        created_at,
        updated_at
    FROM {{ ref('int_form_records_flat') }}
)

SELECT
    rb.record_id,
    rb.record_uid,
    rb.form_id,
    rb.building_id,
    rb.record_state,

    -- Status for data movement
    COALESCE(rb.fulcrum_status, 'unknown') AS assessment_status,

    -- Status grouping for simpler charts
    CASE
        WHEN rb.fulcrum_status IN ('registered') THEN '1. Registered'
        WHEN rb.fulcrum_status IN ('enrolled', 'not_enrolled') THEN '2. Enrollment'
        WHEN rb.fulcrum_status IN ('selection_go', 'selection_nogo') THEN '3. Selection'
        WHEN rb.fulcrum_status IN ('internal_training', 'external_training', 'passed', 'not_passed') THEN '4. Training'
        WHEN rb.fulcrum_status IN ('design_internal_approval', 'design_complete') THEN '5. Design'
        WHEN rb.fulcrum_status LIKE 'tranche%' THEN '6. Financial/Construction'
        WHEN rb.fulcrum_status IN ('incomplete', 'attention_needed', 'withdrawn') THEN '7. Issues'
        ELSE '0. Other'
    END AS status_group,

    -- Location
    COALESCE(cd.country, 'Unknown') AS country,
    COALESCE(bd.branch_location, 'Unknown') AS branch_location,
    rb.fulcrum_latitude AS latitude,
    rb.fulcrum_longitude AS longitude,

    -- Demographics
    COALESCE(gd.gender, 'Not specified') AS gender,

    -- Project
    rb.project_ref_id,

    -- Timestamps
    rb.created_at,
    rb.updated_at,
    DATE_TRUNC('month', rb.created_at::timestamp) AS assessment_month,
    DATE_TRUNC('week', rb.created_at::timestamp) AS assessment_week

FROM record_base rb
LEFT JOIN country_data cd ON rb.record_id = cd.record_id
LEFT JOIN gender_data gd ON rb.record_id = gd.record_id
LEFT JOIN branch_data bd ON rb.record_id = bd.record_id
