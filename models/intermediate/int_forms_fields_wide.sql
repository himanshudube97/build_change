{{
    config(
        materialized='table'
    )
}}

/*
    WIDE FLAT TABLE: One row per form field with ALL attributes as columns

    This table flattens:
    - structure_override: All form field definitions
    - config: Form configuration settings
    - fulcrum: Fulcrum sync metadata

    Optimized for BI tools and visualization (no JOINs needed)
*/

WITH form_base AS (
    SELECT
        _id AS form_id,
        uid AS form_uid,
        name AS form_name,
        category AS form_category,
        description AS form_description,
        display AS form_display,
        origin AS form_origin,
        version AS form_version,
        "order" AS form_order,
        is_active,
        is_public,
        is_mfi,
        is_sub_form,
        is_follow_up,
        is_parent_sub_form,
        are_sub_forms_related,
        project_id,
        created_at,
        updated_at,

        -- Flatten config JSON
        config->>'internal' AS config_internal,
        config->'ui'->>'edit' AS config_ui_edit,
        config->'ui'->>'display' AS config_ui_display,
        config->'ui'->>'instant' AS config_ui_instant,
        config->'ui'->>'generate' AS config_ui_generate,
        config->'ui'->>'multiple' AS config_ui_multiple,
        config->'access'->>'fill_roles' AS config_access_fill_roles,
        config->'access'->>'read_roles' AS config_access_read_roles,
        config->>'export' AS config_export,
        config->'fields' AS config_fields,
        config->'depends_on' AS config_depends_on,
        config->'filtration'->>'keys' AS config_filtration_keys,
        config->'injectables' AS config_injectables,

        -- Flatten fulcrum JSON
        fulcrum->>'form_id' AS fulcrum_form_id,
        (fulcrum->>'version')::numeric AS fulcrum_version,
        fulcrum->'statuses' AS fulcrum_statuses,
        fulcrum->>'form_updated_at' AS fulcrum_form_updated_at,
        fulcrum->>'record_updated_at' AS fulcrum_record_updated_at,

        -- Keep structure_override for field extraction
        structure_override,

        -- Airbyte metadata
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _ab_cdc_cursor,
        _ab_cdc_updated_at,
        _ab_cdc_deleted_at

    FROM {{ source('staging', 'forms') }}
    WHERE structure_override IS NOT NULL
),

-- Extract all elements (both sections and direct fields)
form_elements AS (
    SELECT
        fb.*,
        elem.value AS element,
        elem.ordinality AS element_order
    FROM form_base fb,
    LATERAL jsonb_array_elements(fb.structure_override) WITH ORDINALITY AS elem(value, ordinality)
),

-- Flatten: Get direct fields and fields nested in sections
flattened_fields AS (
    -- Direct fields (non-sections at top level)
    SELECT
        fe.*,
        NULL::text AS section_key,
        NULL::text AS section_label,
        NULL::text AS section_data_name,
        NULL::text AS section_display,
        fe.element AS field_json,
        fe.element_order AS field_order
    FROM form_elements fe
    WHERE fe.element->>'type' != 'Section'

    UNION ALL

    -- Fields within sections
    SELECT
        fe.*,
        fe.element->>'key' AS section_key,
        fe.element->>'label' AS section_label,
        fe.element->>'data_name' AS section_data_name,
        fe.element->>'display' AS section_display,
        nested.value AS field_json,
        (fe.element_order * 1000 + nested.ordinality) AS field_order
    FROM form_elements fe,
    LATERAL jsonb_array_elements(fe.element->'elements') WITH ORDINALITY AS nested(value, ordinality)
    WHERE fe.element->>'type' = 'Section'
      AND fe.element->'elements' IS NOT NULL
)

SELECT
    -- ========== FORM IDENTIFIERS ==========
    form_id,
    form_uid,
    form_name,
    form_category,
    form_description,
    form_display,
    form_origin,
    form_version,
    form_order,

    -- ========== FORM FLAGS ==========
    is_active,
    is_public,
    is_mfi,
    is_sub_form,
    is_follow_up,
    is_parent_sub_form,
    are_sub_forms_related,
    project_id,

    -- ========== FORM TIMESTAMPS ==========
    created_at AS form_created_at,
    updated_at AS form_updated_at,

    -- ========== CONFIG (flattened) ==========
    config_internal,
    config_ui_edit,
    config_ui_display,
    config_ui_instant,
    config_ui_generate,
    config_ui_multiple,
    config_access_fill_roles,
    config_access_read_roles,
    config_export,
    config_fields,
    config_depends_on,
    config_filtration_keys,
    config_injectables,

    -- ========== FULCRUM (flattened) ==========
    fulcrum_form_id,
    fulcrum_version,
    fulcrum_statuses,
    fulcrum_form_updated_at,
    fulcrum_record_updated_at,

    -- ========== SECTION INFO ==========
    section_key,
    section_label,
    section_data_name,
    section_display,

    -- ========== FIELD IDENTIFIERS ==========
    field_json->>'key' AS field_key,
    field_json->>'type' AS field_type,
    field_json->>'label' AS field_label,
    field_json->>'data_name' AS field_data_name,
    field_json->>'description' AS field_description,
    field_order,

    -- ========== FIELD FLAGS ==========
    (field_json->>'required')::boolean AS field_required,
    (field_json->>'hidden')::boolean AS field_hidden,
    (field_json->>'disabled')::boolean AS field_disabled,
    (field_json->>'default_previous_value')::boolean AS field_default_previous_value,

    -- ========== FIELD VALUES ==========
    field_json->>'default_value' AS field_default_value,
    field_json->>'ai_prompt' AS field_ai_prompt,

    -- ========== TEXT FIELD ATTRIBUTES ==========
    (field_json->>'numeric')::boolean AS text_numeric,
    (field_json->>'min_length')::int AS text_min_length,
    (field_json->>'max_length')::int AS text_max_length,
    field_json->>'pattern' AS text_pattern,
    field_json->>'pattern_description' AS text_pattern_description,

    -- ========== CHOICE FIELD ATTRIBUTES ==========
    (field_json->>'multiple')::boolean AS choice_multiple,
    (field_json->>'allow_other')::boolean AS choice_allow_other,
    field_json->>'choice_list_id' AS choice_list_id,
    field_json->'choices' AS choices_json,
    -- Flatten choices to comma-separated string for easy filtering
    (
        SELECT string_agg(c->>'label', ', ')
        FROM jsonb_array_elements(field_json->'choices') AS c
    ) AS choices_labels,
    (
        SELECT string_agg(c->>'value', ', ')
        FROM jsonb_array_elements(field_json->'choices') AS c
    ) AS choices_values,
    jsonb_array_length(COALESCE(field_json->'choices', '[]'::jsonb)) AS choices_count,

    -- ========== YES/NO FIELD ATTRIBUTES ==========
    field_json->'positive'->>'label' AS yesno_positive_label,
    field_json->'positive'->>'value' AS yesno_positive_value,
    field_json->'negative'->>'label' AS yesno_negative_label,
    field_json->'negative'->>'value' AS yesno_negative_value,
    field_json->'neutral'->>'label' AS yesno_neutral_label,
    field_json->'neutral'->>'value' AS yesno_neutral_value,
    (field_json->>'neutral_enabled')::boolean AS yesno_neutral_enabled,

    -- ========== DATE/TIME FIELD ATTRIBUTES ==========
    field_json->>'format' AS datetime_format,
    (field_json->>'min')::int AS datetime_min,
    (field_json->>'max')::int AS datetime_max,

    -- ========== CALCULATED FIELD ATTRIBUTES ==========
    field_json->>'expression' AS calculated_expression,

    -- ========== PHOTO FIELD ATTRIBUTES ==========
    (field_json->>'annotations_enabled')::boolean AS photo_annotations_enabled,
    (field_json->>'timestamp_enabled')::boolean AS photo_timestamp_enabled,
    (field_json->>'latlongstamp_enabled')::boolean AS photo_latlongstamp_enabled,
    (field_json->>'deidentification_enabled')::boolean AS photo_deidentification_enabled,

    -- ========== SIGNATURE FIELD ATTRIBUTES ==========
    field_json->>'agreement_text' AS signature_agreement_text,

    -- ========== CLASSIFICATION FIELD ATTRIBUTES ==========
    field_json->>'classification_set_id' AS classification_set_id,
    field_json->>'empty_label' AS classification_empty_label,

    -- ========== HYPERLINK FIELD ATTRIBUTES ==========
    field_json->>'default_url' AS hyperlink_default_url,

    -- ========== REPEATABLE FIELD ATTRIBUTES ==========
    (field_json->>'geometry_required')::boolean AS repeatable_geometry_required,
    field_json->'geometry_types' AS repeatable_geometry_types,
    field_json->>'title_field_key' AS repeatable_title_field_key,
    field_json->'title_field_keys' AS repeatable_title_field_keys,

    -- ========== VIDEO FIELD ATTRIBUTES ==========
    (field_json->>'audio_enabled')::boolean AS video_audio_enabled,
    (field_json->>'track_enabled')::boolean AS video_track_enabled,

    -- ========== RECORD LINK FIELD ATTRIBUTES ==========
    field_json->>'form_id' AS recordlink_form_id,
    (field_json->>'allow_creating_records')::boolean AS recordlink_allow_create,
    (field_json->>'allow_existing_records')::boolean AS recordlink_allow_existing,
    (field_json->>'allow_multiple_records')::boolean AS recordlink_allow_multiple,
    (field_json->>'allow_updating_records')::boolean AS recordlink_allow_update,
    field_json->'record_conditions' AS recordlink_conditions_json,
    field_json->>'record_conditions_type' AS recordlink_conditions_type,
    field_json->'record_defaults' AS recordlink_defaults_json,

    -- ========== ATTACHMENT FIELD (uses min/max_length already covered) ==========
    -- (no additional attributes needed)

    -- ========== PANEL FIELD ATTRIBUTES (form.io style) ==========
    field_json->>'title' AS panel_title,
    field_json->>'theme' AS panel_theme,
    (field_json->>'collapsible')::boolean AS panel_collapsible,
    (field_json->>'input')::boolean AS panel_input,
    field_json->>'customClass' AS panel_custom_class,
    field_json->'components' AS panel_components_json,
    field_json->'conditional' AS panel_conditional_json,
    field_json->'validate' AS panel_validate_json,

    -- ========== CONDITIONAL LOGIC ==========
    field_json->'visible_conditions' AS visible_conditions_json,
    field_json->>'visible_conditions_type' AS visible_conditions_type,
    field_json->>'visible_conditions_behavior' AS visible_conditions_behavior,
    field_json->'required_conditions' AS required_conditions_json,
    field_json->>'required_conditions_type' AS required_conditions_type,
    -- Flags for easy filtering
    (field_json->'visible_conditions' IS NOT NULL
     AND field_json->'visible_conditions' != 'null'::jsonb
     AND jsonb_array_length(COALESCE(field_json->'visible_conditions', '[]'::jsonb)) > 0
    ) AS has_visible_conditions,
    (field_json->'required_conditions' IS NOT NULL
     AND field_json->'required_conditions' != 'null'::jsonb
     AND jsonb_array_length(COALESCE(field_json->'required_conditions', '[]'::jsonb)) > 0
    ) AS has_required_conditions,

    -- ========== AIRBYTE METADATA ==========
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _ab_cdc_cursor,
    _ab_cdc_updated_at,
    _ab_cdc_deleted_at,

    -- ========== RAW JSON (for debugging/edge cases) ==========
    field_json AS _raw_field_json

FROM flattened_fields
ORDER BY form_name, field_order
