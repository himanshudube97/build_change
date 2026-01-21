{{
    config(
        materialized='table'
    )
}}

/*
    MART: Records by Branch Location

    Detail table for BI filtering - shows each record with its branch location.
    Use this when you need to filter individual responses by branch.

    Grain: One row per record (not per field)
*/

WITH branch_locations AS (
    -- Get branch location for each record
    SELECT
        record_id,
        field_value_text AS branch_location
    FROM {{ ref('int_form_records_flat') }}
    WHERE field_key = 'branchLocation'
      AND field_value_text IS NOT NULL
),

record_base AS (
    -- Get one row per record with key attributes
    SELECT DISTINCT
        record_id,
        record_uid,
        form_id,
        user_id,
        building_id,
        record_state,
        consent,
        created_at,
        updated_at,
        fulcrum_status,
        fulcrum_latitude,
        fulcrum_longitude,
        fulcrum_assignee_name,
        project_ref_id
    FROM {{ ref('int_form_records_flat') }}
)

SELECT
    rb.record_id,
    rb.record_uid,
    rb.form_id,
    rb.user_id,
    rb.building_id,
    bl.branch_location,
    rb.record_state,
    rb.consent,
    rb.created_at,
    rb.updated_at,
    rb.fulcrum_status,
    rb.fulcrum_latitude,
    rb.fulcrum_longitude,
    rb.fulcrum_assignee_name,
    rb.project_ref_id
FROM record_base rb
LEFT JOIN branch_locations bl ON rb.record_id = bl.record_id
ORDER BY rb.created_at DESC
