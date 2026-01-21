{{
    config(
        materialized='table'
    )
}}

/*
    MART: Branch Location Responses

    Shows count of form responses by branch location.
    Use this for filtering and visualizing data by branch.

    Grain: One row per branch location
*/

WITH branch_location_records AS (
    -- Get all records that have a branchLocation field
    SELECT
        record_id,
        record_uid,
        form_id,
        field_value_text AS branch_location,
        record_state,
        created_at,
        updated_at,
        fulcrum_status,
        project_ref_id
    FROM {{ ref('int_form_records_flat') }}
    WHERE field_key = 'branchLocation'
      AND field_value_text IS NOT NULL
      AND field_value_text != ''
)

SELECT
    branch_location,
    COUNT(DISTINCT record_id) AS total_responses,
    COUNT(DISTINCT CASE WHEN record_state = 'submitted' THEN record_id END) AS submitted_responses,
    COUNT(DISTINCT CASE WHEN record_state = 'draft' THEN record_id END) AS draft_responses,
    COUNT(DISTINCT form_id) AS distinct_forms,
    COUNT(DISTINCT project_ref_id) AS distinct_projects,
    MIN(created_at) AS first_response_at,
    MAX(created_at) AS last_response_at
FROM branch_location_records
GROUP BY branch_location
ORDER BY total_responses DESC
