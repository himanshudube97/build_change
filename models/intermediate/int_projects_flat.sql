{{
    config(
        materialized='table'
    )
}}

/*
    FLAT TABLE: Projects with ALL JSON fields flattened

    Flattening approach:
    - Simple objects: Each nested key becomes a column (e.g., config.metrics.enabled → config_metrics_enabled)
    - Simple arrays: Comma-separated strings (e.g., building_use → "Type1, Type2")
    - Arrays of objects: Exploded to rows (phases → steps)

    Grain: One row per STEP per PHASE per PROJECT
    (A project with 2 phases, each having 3 steps = 6 rows)
*/

WITH projects_base AS (
    SELECT
        -- ========== PROJECT IDENTIFIERS ==========
        _id AS project_id,
        uid AS project_uid,
        code AS project_code,
        title AS project_title,

        -- ========== BASIC INFO ==========
        status,
        description,

        -- ========== TIMESTAMPS ==========
        created_at,
        updated_at,

        -- ========== CONFIG - Deeply Flattened ==========
        (
            SELECT string_agg(elem::text, ', ')
            FROM jsonb_array_elements_text(config->'forms') AS elem
        ) AS config_forms,
        (config->'access'->>'public')::boolean AS config_access_public,
        (
            SELECT string_agg(elem::text, ', ')
            FROM jsonb_array_elements_text(config->'access'->'roles') AS elem
        ) AS config_access_roles,
        (config->'metrics'->>'enabled')::boolean AS config_metrics_enabled,
        (config->'metrics'->'dashboard'->>'refresh_rate')::int AS config_metrics_dashboard_refresh_rate,
        (
            SELECT string_agg(elem::text, ', ')
            FROM jsonb_array_elements_text(config->'languages') AS elem
        ) AS config_languages,
        config->>'default_language' AS config_default_language,
        config->>'export_format' AS config_export_format,

        -- ========== FINANCE - Flattened ==========
        (finance->>'max_amount')::numeric AS finance_max_amount,
        (
            SELECT string_agg(elem::text, ', ')
            FROM jsonb_array_elements_text(finance->'tranches') AS elem
        ) AS finance_tranches,
        finance->>'currency' AS finance_currency,

        -- ========== PROGRAMS - Flattened ==========
        (
            SELECT string_agg(elem::text, ', ')
            FROM jsonb_array_elements_text(programs->'regions') AS elem
        ) AS programs_regions,
        (
            SELECT string_agg(elem::text, ', ')
            FROM jsonb_array_elements_text(programs->'countries') AS elem
        ) AS programs_countries,

        -- ========== TIMELINE - Flattened ==========
        timeline->>'start_date' AS timeline_start_date,
        timeline->>'end_date' AS timeline_end_date,
        timeline->>'description' AS timeline_description,

        -- ========== COVER PHOTO - Flattened ==========
        cover_photo->>'url' AS cover_photo_url,
        cover_photo->>'name' AS cover_photo_name,
        cover_photo->>'thumbnail' AS cover_photo_thumbnail,

        -- ========== ENGINEERING - Flattened ==========
        engineering->>'design_orientation' AS engineering_design_orientation,
        (engineering->>'geometry_assessment')::boolean AS engineering_geometry_assessment,

        -- ========== METADATA - Flattened ==========
        metadata->>'source' AS metadata_source,
        metadata->>'version' AS metadata_version,

        -- ========== SIMPLE ARRAYS → Comma-separated ==========
        (
            SELECT string_agg(elem::text, ', ')
            FROM jsonb_array_elements_text(building_use) AS elem
        ) AS building_use_list,
        (
            SELECT string_agg(elem::text, ', ')
            FROM jsonb_array_elements_text(project_tool) AS elem
        ) AS project_tool_list,
        (
            SELECT string_agg(elem::text, ', ')
            FROM jsonb_array_elements_text(type_of_fund) AS elem
        ) AS type_of_fund_list,
        (
            SELECT string_agg(elem::text, ', ')
            FROM jsonb_array_elements_text(project_scope) AS elem
        ) AS project_scope_list,
        (
            SELECT string_agg(elem::text, ', ')
            FROM jsonb_array_elements_text(project_hazard) AS elem
        ) AS project_hazard_list,
        (
            SELECT string_agg(elem::text, ', ')
            FROM jsonb_array_elements_text(project_context) AS elem
        ) AS project_context_list,
        (
            SELECT string_agg(elem::text, ', ')
            FROM jsonb_array_elements_text(project_approach) AS elem
        ) AS project_approach_list,

        -- ========== LOCATIONS - Flattened (first location or comma-separated) ==========
        (
            SELECT string_agg(loc->>'name', ', ')
            FROM jsonb_array_elements(locations) AS loc
        ) AS locations_names,
        (locations->0->>'latitude')::numeric AS location_first_latitude,
        (locations->0->>'longitude')::numeric AS location_first_longitude,
        jsonb_array_length(COALESCE(locations, '[]'::jsonb)) AS locations_count,

        -- ========== PHASE CATEGORIES - Flattened ==========
        (
            SELECT string_agg(pc->>'name', ', ')
            FROM jsonb_array_elements(phase_categories) AS pc
        ) AS phase_categories_names,
        jsonb_array_length(COALESCE(phase_categories, '[]'::jsonb)) AS phase_categories_count,

        -- ========== AIRBYTE METADATA ==========
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _ab_cdc_cursor,
        _ab_cdc_updated_at,
        _ab_cdc_deleted_at,

        -- Keep phases for explosion
        phases

    FROM {{ source('staging', 'projects') }}
),

-- Explode phases array
projects_phases AS (
    SELECT
        pb.*,
        phase.ordinality AS phase_order,
        phase.value AS phase_json
    FROM projects_base pb
    LEFT JOIN LATERAL jsonb_array_elements(pb.phases) WITH ORDINALITY AS phase(value, ordinality) ON TRUE
),

-- Explode steps within each phase
phases_steps AS (
    SELECT
        pp.*,
        step.ordinality AS step_order,
        step.value AS step_json
    FROM projects_phases pp
    LEFT JOIN LATERAL jsonb_array_elements(pp.phase_json->'steps') WITH ORDINALITY AS step(value, ordinality) ON TRUE
)

SELECT
    -- ========== PROJECT IDENTIFIERS ==========
    project_id,
    project_uid,
    project_code,
    project_title,

    -- ========== BASIC INFO ==========
    status,
    description,

    -- ========== TIMESTAMPS ==========
    created_at,
    updated_at,

    -- ========== CONFIG (Deeply Flattened) ==========
    config_forms,
    config_access_public,
    config_access_roles,
    config_metrics_enabled,
    config_metrics_dashboard_refresh_rate,
    config_languages,
    config_default_language,
    config_export_format,

    -- ========== FINANCE (Flattened) ==========
    finance_max_amount,
    finance_tranches,
    finance_currency,

    -- ========== PROGRAMS (Flattened) ==========
    programs_regions,
    programs_countries,

    -- ========== TIMELINE (Flattened) ==========
    timeline_start_date,
    timeline_end_date,
    timeline_description,

    -- ========== COVER PHOTO (Flattened) ==========
    cover_photo_url,
    cover_photo_name,
    cover_photo_thumbnail,

    -- ========== ENGINEERING (Flattened) ==========
    engineering_design_orientation,
    engineering_geometry_assessment,

    -- ========== METADATA (Flattened) ==========
    metadata_source,
    metadata_version,

    -- ========== SIMPLE ARRAYS (Comma-separated) ==========
    building_use_list,
    project_tool_list,
    type_of_fund_list,
    project_scope_list,
    project_hazard_list,
    project_context_list,
    project_approach_list,

    -- ========== LOCATIONS (Flattened) ==========
    locations_names,
    location_first_latitude,
    location_first_longitude,
    locations_count,

    -- ========== PHASE CATEGORIES (Flattened) ==========
    phase_categories_names,
    phase_categories_count,

    -- ========== PHASE (Exploded) ==========
    phase_order,
    phase_json->>'key' AS phase_key,
    phase_json->>'name' AS phase_name,
    phase_json->>'description' AS phase_description,
    phase_json->>'icon' AS phase_icon,
    phase_json->>'category' AS phase_category,
    (
        SELECT string_agg(s->>'label', ', ')
        FROM jsonb_array_elements(phase_json->'statuses') AS s
    ) AS phase_statuses_labels,
    jsonb_array_length(COALESCE(phase_json->'steps', '[]'::jsonb)) AS phase_steps_count,

    -- ========== STEP (Exploded) ==========
    step_order,
    step_json->>'key' AS step_key,
    step_json->>'name' AS step_name,
    step_json->>'description' AS step_description,
    (step_json->>'required')::boolean AS step_required,
    (step_json->>'optional')::boolean AS step_optional,
    (
        SELECT string_agg(f->>'form_id', ', ')
        FROM jsonb_array_elements(step_json->'forms') AS f
    ) AS step_form_ids,
    (
        SELECT string_agg(f->>'label', ', ')
        FROM jsonb_array_elements(step_json->'forms') AS f
    ) AS step_form_labels,
    jsonb_array_length(COALESCE(step_json->'forms', '[]'::jsonb)) AS step_forms_count,

    -- ========== AIRBYTE METADATA ==========
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _ab_cdc_cursor,
    _ab_cdc_updated_at,
    _ab_cdc_deleted_at

FROM phases_steps
ORDER BY project_title, phase_order, step_order
