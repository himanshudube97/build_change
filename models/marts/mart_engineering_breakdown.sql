{{
    config(
        materialized='table'
    )
}}

/*
    MART: Engineering Breakdown

    Shows distribution of construction systems, wall types, roof types.
    Use for pie/donut charts showing engineering choices.

    Grain: One row per engineering_type + value combination
*/

WITH construction_system AS (
    SELECT
        record_id,
        'Construction System' AS engineering_type,
        field_value_text AS engineering_value
    FROM {{ ref('int_form_records_flat') }}
    WHERE field_key IN ('7760', '5989')  -- Construction system field keys
      AND field_value_text IS NOT NULL
      AND field_value_text != ''
),

masonry_type AS (
    SELECT
        record_id,
        'Masonry Type' AS engineering_type,
        field_value_text AS engineering_value
    FROM {{ ref('int_form_records_flat') }}
    WHERE field_key IN ('6770', '5306')  -- Main type of masonry field keys
      AND field_value_text IS NOT NULL
      AND field_value_text != ''
),

building_materials AS (
    SELECT
        record_id,
        'Building Materials' AS engineering_type,
        field_value_text AS engineering_value
    FROM {{ ref('int_form_records_flat') }}
    WHERE field_key IN ('3ec5', 'fc50')  -- Building materials field keys
      AND field_value_text IS NOT NULL
      AND field_value_text != ''
),

all_engineering AS (
    SELECT * FROM construction_system
    UNION ALL
    SELECT * FROM masonry_type
    UNION ALL
    SELECT * FROM building_materials
)

SELECT
    engineering_type,
    engineering_value,
    COUNT(DISTINCT record_id) AS record_count
FROM all_engineering
GROUP BY engineering_type, engineering_value
ORDER BY engineering_type, record_count DESC
