# Plan: Flattening `structure_override` JSON in Forms Table

## Executive Summary

The `staging.forms` table contains a `structure_override` JSONB column with deeply nested form field definitions. This plan outlines a dbt-based approach to flatten this data into queryable, analytics-ready tables.

---

## Current State Analysis

### Source Data: `staging.forms`
- **Total forms**: 9
- **Forms with structure_override**: 9 (100%)
- **Total fields across all forms**: ~1,441

### JSON Structure Overview

```
structure_override (array)
├── Section
│   ├── key, label, data_name, display, hidden, disabled
│   └── elements[] (nested fields)
│       ├── ChoiceField (with choices[])
│       ├── TextField
│       ├── DateTimeField
│       ├── YesNoField (with positive/negative/neutral objects)
│       ├── PhotoField
│       ├── SignatureField
│       ├── CalculatedField (with expression)
│       ├── ClassificationField
│       ├── RecordLinkField (with record_conditions[])
│       ├── Label
│       └── TimeField
└── Direct fields (non-sectioned)
```

### Field Types Discovered (12 types)
| Type | Description | Special Attributes |
|------|-------------|-------------------|
| Section | Container for grouped fields | `elements[]`, `display` |
| ChoiceField | Dropdown/radio selection | `choices[]`, `multiple`, `allow_other`, `choice_list_id` |
| TextField | Text input | `numeric`, `min_length`, `max_length`, `pattern` |
| DateTimeField | Date/time picker | `format` |
| TimeField | Time-only picker | `format` |
| YesNoField | Binary/ternary choice | `positive{}`, `negative{}`, `neutral{}`, `neutral_enabled` |
| PhotoField | Image capture | `annotations_enabled`, `timestamp_enabled` |
| SignatureField | Signature capture | `agreement_text` |
| CalculatedField | Computed value | `expression` |
| ClassificationField | Hierarchical selection | `classification_set_id` |
| RecordLinkField | Link to other records | `form_id`, `record_conditions[]`, `record_defaults[]` |
| Label | Display-only text | - |

### Common Field Attributes (present in most fields)
- `key` - Unique field identifier
- `type` - Field type
- `label` - Display label
- `data_name` - Internal/API name
- `required` - Is field required
- `hidden` - Is field hidden
- `disabled` - Is field disabled
- `default_value` - Default value
- `description` - Field description
- `visible_conditions[]` - Conditional visibility rules
- `required_conditions[]` - Conditional requirement rules
- `default_previous_value` - Use previous value as default

---

## Proposed Architecture

### Design Principles
1. **Star Schema**: Core fact table with dimension tables for nested arrays
2. **Layered Approach**: Staging → Intermediate → Mart
3. **Preserve Flexibility**: Keep type-specific attributes as JSONB for extensibility
4. **Query Performance**: Flatten commonly queried attributes into columns

### Model Dependency Graph

```
staging.forms
    │
    ▼
┌───────────────────────────────────────────────────────────┐
│                   INTERMEDIATE LAYER                       │
├───────────────────────────────────────────────────────────┤
│                                                           │
│  int_forms_fields (core flattened table)                  │
│      │                                                    │
│      ├──► int_forms_field_choices                         │
│      │    (one row per choice option)                     │
│      │                                                    │
│      ├──► int_forms_field_conditions                      │
│      │    (one row per visibility/required condition)     │
│      │                                                    │
│      └──► int_forms_yesno_options                         │
│           (one row per yes/no/neutral option)             │
│                                                           │
└───────────────────────────────────────────────────────────┘
    │
    ▼
┌───────────────────────────────────────────────────────────┐
│                      MART LAYER                            │
├───────────────────────────────────────────────────────────┤
│                                                           │
│  mart_form_schema_wide                                    │
│  (pivoted view with one row per form, columns per field)  │
│                                                           │
│  mart_form_field_summary                                  │
│  (aggregated stats per form)                              │
│                                                           │
└───────────────────────────────────────────────────────────┘
```

---

## Detailed Model Specifications

### 1. `int_forms_fields` (Core Fact Table)

**Purpose**: One row per form field, flattening all nested elements

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| form_id | VARCHAR | Form identifier (`_id`) |
| form_name | VARCHAR | Form name |
| form_uid | VARCHAR | Form UID |
| section_key | VARCHAR | Parent section key (NULL if not in section) |
| section_label | VARCHAR | Parent section label |
| section_data_name | VARCHAR | Parent section data_name |
| field_key | VARCHAR | Unique field key |
| field_type | VARCHAR | Field type (ChoiceField, TextField, etc.) |
| field_label | VARCHAR | Display label |
| data_name | VARCHAR | Internal/API name |
| description | TEXT | Field description |
| is_required | BOOLEAN | Is field required |
| is_hidden | BOOLEAN | Is field hidden |
| is_disabled | BOOLEAN | Is field disabled |
| default_value | VARCHAR | Default value |
| default_previous_value | BOOLEAN | Use previous value as default |
| field_order | INTEGER | Position within form |
| has_choices | BOOLEAN | Does field have choices array |
| has_visible_conditions | BOOLEAN | Has visibility conditions |
| has_required_conditions | BOOLEAN | Has requirement conditions |
| type_specific_config | JSONB | All type-specific attributes |

**Key SQL Pattern** (PostgreSQL):
```sql
WITH form_elements AS (
    SELECT
        f._id AS form_id,
        f.name AS form_name,
        f.uid AS form_uid,
        elem.value AS element,
        elem.ordinality AS element_order
    FROM staging.forms f,
    LATERAL jsonb_array_elements(f.structure_override) WITH ORDINALITY AS elem(value, ordinality)
    WHERE f.structure_override IS NOT NULL
),
-- Flatten sections and their nested elements
flattened_fields AS (
    -- Direct fields (non-sections)
    SELECT
        form_id, form_name, form_uid,
        NULL AS section_key,
        NULL AS section_label,
        element AS field,
        element_order
    FROM form_elements
    WHERE element->>'type' != 'Section'

    UNION ALL

    -- Fields within sections
    SELECT
        fe.form_id, fe.form_name, fe.form_uid,
        fe.element->>'key' AS section_key,
        fe.element->>'label' AS section_label,
        nested.value AS field,
        fe.element_order * 1000 + nested.ordinality AS element_order
    FROM form_elements fe,
    LATERAL jsonb_array_elements(fe.element->'elements') WITH ORDINALITY AS nested(value, ordinality)
    WHERE fe.element->>'type' = 'Section'
)
SELECT
    form_id,
    form_name,
    form_uid,
    section_key,
    section_label,
    field->>'key' AS field_key,
    field->>'type' AS field_type,
    field->>'label' AS field_label,
    field->>'data_name' AS data_name,
    field->>'description' AS description,
    (field->>'required')::boolean AS is_required,
    (field->>'hidden')::boolean AS is_hidden,
    (field->>'disabled')::boolean AS is_disabled,
    field->>'default_value' AS default_value,
    (field->>'default_previous_value')::boolean AS default_previous_value,
    element_order AS field_order,
    jsonb_array_length(COALESCE(field->'choices', '[]'::jsonb)) > 0 AS has_choices,
    field->'visible_conditions' IS NOT NULL AND field->'visible_conditions' != 'null'::jsonb AS has_visible_conditions,
    field->'required_conditions' IS NOT NULL AND field->'required_conditions' != 'null'::jsonb AS has_required_conditions,
    field AS type_specific_config
FROM flattened_fields
```

---

### 2. `int_forms_field_choices` (Choice Options)

**Purpose**: One row per choice option for ChoiceField/ClassificationField

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| form_id | VARCHAR | Form identifier |
| field_key | VARCHAR | Field key |
| field_label | VARCHAR | Field label |
| choice_order | INTEGER | Position in choices array |
| choice_label | VARCHAR | Choice display label |
| choice_value | VARCHAR | Choice value |

**Key SQL Pattern**:
```sql
SELECT
    ff.form_id,
    ff.field_key,
    ff.field_label,
    choice.ordinality AS choice_order,
    choice.value->>'label' AS choice_label,
    choice.value->>'value' AS choice_value
FROM int_forms_fields ff,
LATERAL jsonb_array_elements(ff.type_specific_config->'choices') WITH ORDINALITY AS choice(value, ordinality)
WHERE ff.has_choices = true
```

---

### 3. `int_forms_field_conditions` (Visibility/Required Conditions)

**Purpose**: One row per condition rule

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| form_id | VARCHAR | Form identifier |
| field_key | VARCHAR | Field key |
| condition_type | VARCHAR | 'visible', 'required', or 'record' |
| condition_order | INTEGER | Position in conditions array |
| reference_field_key | VARCHAR | Field key being referenced |
| operator | VARCHAR | Comparison operator |
| condition_value | VARCHAR | Value to compare against |
| conditions_logic | VARCHAR | 'all' or 'any' |

---

### 4. `int_forms_yesno_options` (YesNo Field Options)

**Purpose**: One row per yes/no/neutral option

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| form_id | VARCHAR | Form identifier |
| field_key | VARCHAR | Field key |
| option_type | VARCHAR | 'positive', 'negative', 'neutral' |
| option_label | VARCHAR | Display label |
| option_value | VARCHAR | Value |
| is_enabled | BOOLEAN | Is option enabled |

---

### 5. `mart_form_field_summary` (Analytics Summary)

**Purpose**: High-level form analytics

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| form_id | VARCHAR | Form identifier |
| form_name | VARCHAR | Form name |
| total_fields | INTEGER | Total field count |
| required_fields | INTEGER | Required field count |
| hidden_fields | INTEGER | Hidden field count |
| section_count | INTEGER | Number of sections |
| field_types | JSONB | Count by field type |
| has_conditional_logic | BOOLEAN | Any conditions present |

---

## File Structure

```
models/
├── intermediate/
│   ├── _int_models.yml          # Schema documentation
│   ├── int_forms_fields.sql
│   ├── int_forms_field_choices.sql
│   ├── int_forms_field_conditions.sql
│   └── int_forms_yesno_options.sql
└── marts/
    ├── _mart_models.yml         # Schema documentation
    ├── mart_form_field_summary.sql
    └── mart_form_schema_wide.sql (optional)
```

---

## Implementation Order

1. **Phase 1**: `int_forms_fields` - Core flattening (highest value)
2. **Phase 2**: `int_forms_field_choices` - Flatten choices
3. **Phase 3**: `int_forms_field_conditions` - Flatten conditions
4. **Phase 4**: `int_forms_yesno_options` - Flatten yes/no options
5. **Phase 5**: Mart layer models (as needed)

---

## Alternatives Considered

### Option A: Single Wide Table
Flatten everything into one table with many NULL columns.
- **Pros**: Simple, single table
- **Cons**: Very wide, many NULLs, choices still need array handling

### Option B: Fully Normalized (6NF-style)
Create separate tables for every attribute type.
- **Pros**: No redundancy, very flexible
- **Cons**: Too many JOINs, complex queries, over-engineered

### Option C: Document Store Approach (Chosen elements)
Keep `type_specific_config` as JSONB for flexibility.
- **Pros**: Extensible, handles unknown future attributes
- **Cons**: Requires JSON functions for deep queries

**Decision**: Hybrid approach (Option C) - flatten common attributes for easy querying while preserving type-specific configs as JSONB for flexibility.

---

## Usage Examples

### Query 1: Get all required fields for a form
```sql
SELECT field_label, field_type, data_name
FROM intermediate.int_forms_fields
WHERE form_name = 'My Form'
  AND is_required = true
ORDER BY field_order;
```

### Query 2: Get all choice options for dropdown fields
```sql
SELECT
    ff.form_name,
    ff.field_label,
    fc.choice_label,
    fc.choice_value
FROM intermediate.int_forms_fields ff
JOIN intermediate.int_forms_field_choices fc
    ON ff.form_id = fc.form_id AND ff.field_key = fc.field_key
WHERE ff.field_type = 'ChoiceField';
```

### Query 3: Find fields with conditional visibility
```sql
SELECT
    ff.form_name,
    ff.field_label,
    cond.reference_field_key,
    cond.operator,
    cond.condition_value
FROM intermediate.int_forms_fields ff
JOIN intermediate.int_forms_field_conditions cond
    ON ff.form_id = cond.form_id AND ff.field_key = cond.field_key
WHERE cond.condition_type = 'visible';
```

---

## Next Steps

1. [ ] Review and approve this plan
2. [ ] Create `models/intermediate/` directory structure
3. [ ] Implement `int_forms_fields.sql`
4. [ ] Test with `dbt run --select int_forms_fields`
5. [ ] Implement remaining intermediate models
6. [ ] Add schema tests and documentation
7. [ ] Implement mart layer (if needed)

---

## Questions for Decision

1. **Materialization**: Should intermediate models be `view` or `table`?
   - Recommend: `table` for performance (data is small)

2. **Incremental**: Should we implement incremental logic?
   - Recommend: Not needed for 9 forms, but can add later

3. **Additional marts**: Any specific analytics views needed beyond the summary?
