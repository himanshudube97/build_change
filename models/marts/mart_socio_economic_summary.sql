{{
    config(
        materialized='table'
    )
}}

/*
    MART: Socio Economic Summary (Aggregated)

    Pre-aggregated counts for quick dashboard loading.
    Shows assessment counts by status, location, gender.

    Grain: One row per country + branch + status_group + gender
*/

SELECT
    country,
    branch_location,
    status_group,
    assessment_status,
    gender,
    assessment_month,

    COUNT(*) AS total_assessments,
    COUNT(DISTINCT building_id) AS distinct_buildings,
    COUNT(DISTINCT form_id) AS distinct_forms,
    MIN(created_at) AS first_assessment,
    MAX(created_at) AS last_assessment

FROM {{ ref('mart_socio_economic_assessments') }}
GROUP BY
    country,
    branch_location,
    status_group,
    assessment_status,
    gender,
    assessment_month
ORDER BY
    country,
    branch_location,
    status_group,
    gender
