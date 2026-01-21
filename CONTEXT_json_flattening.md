# JSON Flattening Implementation

## Overview

This document describes the JSON flattening approach used in this dbt project to convert nested JSONB columns into flat, analytics-ready tables.

**Warehouse:** PostgreSQL
**Date:** January 2025

---

## Flattening Rules

| JSON Type | Flattening Approach | Example |
|-----------|---------------------|---------|
| Nested object | Each key becomes a column with `_` separator | `config.metrics.enabled` → `config_metrics_enabled` |
| Simple array | Comma-separated string | `["a", "b"]` → `"a, b"` |
| Array of objects | Explode to rows (one row per array element) | `phases[{}, {}]` → 2 rows |

---

## Models Created

### 1. `int_forms_fields_wide.sql`
- **Source:** `staging.forms`
- **Grain:** One row per **field** per form
- **Rows:** 256
- **Explodes:** `structure_override` array (form field definitions)

### 2. `int_form_records_flat.sql`
- **Source:** `staging.form_records`
- **Grain:** One row per **field value** per record
- **Rows:** 279,840
- **Explodes:** `data` object (unpivots dynamic key-value pairs to rows)
- **Flattens:** `cnf`, `fulcrum`, `project`, `metadata`, `zipped_file`

### 3. `int_projects_flat.sql`
- **Source:** `staging.projects`
- **Grain:** One row per **step** per **phase** per project
- **Rows:** 17
- **Explodes:** `phases[]` → `steps[]` (double explosion)
- **Flattens:** `config`, `finance`, `programs`, `timeline`, `cover_photo`, `engineering`, `metadata`, `locations`, `phase_categories`

---

## SQL Patterns Used

### Pattern 1: Simple Object Flattening
```sql
-- Nested object → columns
config->>'default_language' AS config_default_language,
config->'metrics'->>'enabled' AS config_metrics_enabled,
config->'metrics'->'dashboard'->>'refresh_rate' AS config_metrics_dashboard_refresh_rate
```

### Pattern 2: Array to Comma-Separated String
```sql
-- Array of primitives → single string column
(
    SELECT string_agg(elem::text, ', ')
    FROM jsonb_array_elements_text(config->'languages') AS elem
) AS config_languages
```

### Pattern 3: Array of Objects Explosion (LATERAL JOIN)
```sql
-- Explode array to rows
SELECT
    pb.*,
    phase.ordinality AS phase_order,
    phase.value AS phase_json
FROM projects_base pb
LEFT JOIN LATERAL jsonb_array_elements(pb.phases)
    WITH ORDINALITY AS phase(value, ordinality) ON TRUE
```

### Pattern 4: Double Explosion (Nested Arrays)
```sql
-- Explode phases, then explode steps within each phase
WITH projects_phases AS (
    SELECT pb.*, phase.value AS phase_json
    FROM projects_base pb
    LEFT JOIN LATERAL jsonb_array_elements(pb.phases) AS phase ON TRUE
),
phases_steps AS (
    SELECT pp.*, step.value AS step_json
    FROM projects_phases pp
    LEFT JOIN LATERAL jsonb_array_elements(pp.phase_json->'steps') AS step ON TRUE
)
SELECT ... FROM phases_steps
```

### Pattern 5: Dynamic Key-Value Unpivoting
```sql
-- Convert {key1: val1, key2: val2} to rows
SELECT
    frb.*,
    kv.key AS field_key,
    kv.value AS field_value_json,
    CASE
        WHEN jsonb_typeof(kv.value) = 'string' THEN kv.value #>> '{}'
        WHEN jsonb_typeof(kv.value) = 'number' THEN kv.value::text
        ELSE kv.value::text
    END AS field_value_text
FROM form_records_base frb,
LATERAL jsonb_each(frb.data) AS kv(key, value)
```

---

## Important Notes

### Schema is Fixed at Build Time
- PostgreSQL tables have fixed schemas
- New JSON keys will **NOT** automatically become columns
- You must manually add new columns and run `dbt run` to pick up new keys
- Keep `_raw_*_json` columns for accessing fields not yet flattened

### Handling Missing Columns
- If a JSON key doesn't exist, `->>'key'` returns `NULL` (not an error)
- This allows defining columns for all possible keys across all record types
- Rows without that key simply have `NULL` in that column

### Grain Considerations
- Exploding arrays multiplies row count
- Be careful with multiple array explosions (Cartesian product risk)
- Each model should have a clear grain documented in comments

---

## File Structure

```
models/intermediate/
├── _sources.yml                  # Source definitions
├── int_forms_fields_wide.sql     # Forms flattened
├── int_form_records_flat.sql     # Form records flattened
└── int_projects_flat.sql         # Projects flattened
```

---

## Future Considerations

1. **New JSON keys:** Periodically review raw JSON to discover new keys and add columns
2. **Performance:** For large tables, consider incremental materialization
3. **Testing:** Add dbt tests to validate flattening (not_null on key columns, etc.)
