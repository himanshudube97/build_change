{{
    config(
        materialized='table'
    )
}}

/*
    MART: Data Pipeline Status (Data Movement)

    Shows how many records are at each stage of the pipeline.
    Use for funnel charts and understanding data flow.

    Grain: One row per fulcrum_status
*/

WITH record_statuses AS (
    SELECT DISTINCT
        record_id,
        fulcrum_status,
        record_state,
        form_id,
        project_ref_id,
        created_at,
        updated_at
    FROM {{ ref('int_form_records_flat') }}
)

SELECT
    COALESCE(fulcrum_status, 'unknown') AS pipeline_status,
    COUNT(DISTINCT record_id) AS total_records,
    COUNT(DISTINCT form_id) AS distinct_forms,
    COUNT(DISTINCT project_ref_id) AS distinct_projects,
    MIN(created_at) AS earliest_record,
    MAX(created_at) AS latest_record,
    -- Order for funnel visualization
    CASE fulcrum_status
        WHEN 'registered' THEN 1
        WHEN 'enrolled' THEN 2
        WHEN 'selection_go' THEN 3
        WHEN 'selection_nogo' THEN 4
        WHEN 'internal_training' THEN 5
        WHEN 'external_training' THEN 6
        WHEN 'passed' THEN 7
        WHEN 'not_passed' THEN 8
        WHEN 'design_internal_approval' THEN 9
        WHEN 'design_complete' THEN 10
        WHEN 'tranche_internal_approval' THEN 11
        WHEN 'tranche_construction_complete' THEN 12
        WHEN 'incomplete' THEN 90
        WHEN 'attention_needed' THEN 91
        WHEN 'not_enrolled' THEN 92
        WHEN 'withdrawn' THEN 93
        ELSE 99
    END AS status_order
FROM record_statuses
GROUP BY fulcrum_status
ORDER BY status_order
