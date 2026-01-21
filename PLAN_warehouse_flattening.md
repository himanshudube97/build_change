# Comprehensive Plan: Flattening All JSON Columns in Warehouse

## Executive Summary

This plan outlines the strategy to flatten all JSONB columns across 5 tables in the warehouse, creating wide, analytics-ready tables optimized for BI tools and visualization.

---

## Current State Analysis

### Tables Overview

| Table | Rows | JSONB Columns | Complexity | Priority |
|-------|------|---------------|------------|----------|
| **form_records** | 6,478 | 6 | HIGH | 1 |
| **forms** | 9 | 7 | HIGH | 2 (DONE) |
| **project_records** | 2,292 | 14 | HIGH | 3 |
| **projects** | 2 | 17 | MEDIUM | 4 |
| **users** | 162 | 5 | LOW | 5 |

### Total Scope
- **5 tables** to flatten
- **49 JSONB columns** total
- **~9,000 rows** to process
- **~20+ dbt models** to create

---

## Table-by-Table Analysis

### 1. FORMS (COMPLETED)

**Status**: Done - `int_forms_fields_wide` created

**Result**: 119 columns, 256 rows (one per form field)

---

### 2. FORM_RECORDS (Priority 1 - Most Important)

**Row Count**: 6,478 form submissions

**JSONB Columns**:
| Column | Type | Content |
|--------|------|---------|
| `data` | Object | **Dynamic form field values** - keys are field_key from forms |
| `cnf` | Object | Display config, identities, identifiers |
| `fulcrum` | Object | Sync metadata (status, lat/long, record_id, version) |
| `project` | Object | Project reference (id, assignees) |
| `metadata` | Object | Additional metadata (mostly NULL) |
| `zipped_file` | Object | File references (mostly NULL) |

**Challenge**: The `data` column is DYNAMIC - each form has different fields!

**Proposed Models**:

#### Model 2a: `int_form_records_wide` (Main flat table)
Flatten all static JSONB columns into a wide table.

```
Columns (~50):
- Record identifiers (_id, uid, form_id, user_id, building_id)
- Record state (state, consent, is_edited)
- Timestamps (created_at, updated_at)
- CNF flattened (cnf_display, cnf_identities, cnf_identifiers)
- Fulcrum flattened (fulcrum_status, fulcrum_latitude, fulcrum_longitude,
                     fulcrum_record_id, fulcrum_version, fulcrum_assignee_name,
                     fulcrum_current_tranche, fulcrum_record_updated_at)
- Project flattened (project_id, project_assignees)
- Data column kept as JSONB for dynamic access
```

#### Model 2b: `int_form_records_data_unpivoted` (Dynamic data flattened)
Unpivot the dynamic `data` column to long format for analysis.

```
Columns:
- record_id
- form_id
- field_key
- field_value (as text)
- field_value_json (as jsonb for complex values)
```

#### Model 2c: `int_form_records_with_field_labels` (Data joined with field definitions)
Join unpivoted data with form field definitions for human-readable analysis.

```
Columns:
- record_id
- form_name
- section_label
- field_label
- field_type
- field_value
- field_data_name
```

#### Model 2d: `mart_form_submissions_pivot_[form_name]` (Per-form wide tables)
For each form, create a dedicated wide table with actual field names as columns.

Example for "Registration Form":
```
Columns:
- record_id
- country
- language
- registration_date
- stakeholder_name
- stakeholder_phone
- ... (all fields as columns)
```

---

### 3. PROJECT_RECORDS (Priority 3)

**Row Count**: 2,292 project tracking records

**JSONB Columns**:
| Column | Type | Content |
|--------|------|---------|
| `stats` | Object | Record statistics (has_records, total_steps, etc.) |
| `steps` | Array | Step completion status (key, status, affected_at, affected_by) |
| `phases` | Array | Phase completion status |
| `actions` | Object | Action tracking (participants, timestamps, users) |
| `details` | Object | Additional details (mostly NULL) |
| `project` | Object | Project reference |
| `records` | Array | Linked form records |
| `location` | Object | GeoJSON location data |
| `searchable` | Array | Search terms |
| `data_points` | Array | Key metrics/data points |
| `draft_records` | Array | Draft records (mostly NULL) |
| `status_history` | Array | Status change history |
| `deleted_records` | Array | Deleted records (mostly NULL) |
| `duplicate_records` | Array | Duplicate records (mostly NULL) |

**Proposed Models**:

#### Model 3a: `int_project_records_wide` (Main flat table)
```
Columns (~60):
- Record identifiers (_id, uid, label, status, version)
- Current state (current_step, current_phase)
- Timestamps (created_at, updated_at)
- Stats flattened (stats_has_records, stats_total_steps, stats_total_records,
                   stats_total_completed_steps, stats_current_tranche, etc.)
- Actions flattened (actions_participants, actions_last_updated_at,
                     actions_last_updated_by, actions_first_created_at, etc.)
- Project flattened (project_id, project_assignees)
- Location flattened (location_type, location_latitude, location_longitude,
                      location_bbox, location_properties)
- Array columns as JSONB (for later expansion)
```

#### Model 3b: `int_project_records_steps` (Steps flattened)
One row per step per project record.
```
Columns:
- project_record_id
- step_key
- step_status
- step_affected_at
- step_affected_by
- step_order
```

#### Model 3c: `int_project_records_phases` (Phases flattened)
One row per phase per project record.
```
Columns:
- project_record_id
- phase_key
- phase_status
- phase_affected_at
- phase_affected_by
```

#### Model 3d: `int_project_records_data_points` (Data points flattened)
One row per data point per project record.
```
Columns:
- project_record_id
- data_point_key
- data_point_label
- data_point_value
- data_point_source
- is_id
- is_filter
- is_metric
```

#### Model 3e: `int_project_records_status_history` (Status history flattened)
One row per status change.
```
Columns:
- project_record_id
- status
- changed_at
- changed_by
- status_order
```

#### Model 3f: `int_project_records_linked_forms` (Linked form records)
One row per linked form record.
```
Columns:
- project_record_id
- form_record_id
- form_id
- form_label
- consent
- user_id
- updated_at
```

---

### 4. PROJECTS (Priority 4)

**Row Count**: 2 projects

**JSONB Columns**:
| Column | Type | Content |
|--------|------|---------|
| `config` | Object | Project configuration (forms, access, metrics, languages) |
| `phases` | Array | Phase definitions with steps |
| `finance` | Object | Finance settings (tranches, max amount) |
| `metadata` | Object | Additional metadata |
| `programs` | Object | Programs info (regions, countries) |
| `timeline` | Object | Project timeline (start/end dates) |
| `locations` | Array | Project locations |
| `cover_photo` | Object | Cover photo reference |
| `engineering` | Object | Engineering settings |
| `building_use` | Array | Building use types |
| `project_tool` | Array | Project tools used |
| `type_of_fund` | Array | Funding types |
| `project_scope` | Array | Project scope items |
| `project_hazard` | Array | Hazard types |
| `project_context` | Array | Project context |
| `phase_categories` | Array | Phase category definitions |
| `project_approach` | Array | Project approaches |

**Proposed Models**:

#### Model 4a: `int_projects_wide` (Main flat table)
```
Columns (~80):
- Project identifiers (_id, uid, code, title)
- Basic info (budget, health, status, description)
- Flags (is_active, is_mfi, is_archived, is_featured, etc.)
- Timestamps (created_at, updated_at)
- Config flattened (config_forms, config_access, config_metrics, config_languages)
- Finance flattened (finance_tranches, finance_max_amount, etc.)
- Programs flattened (programs_regions, programs_countries)
- Timeline flattened (timeline_start_date, timeline_end_date, timeline_description)
- Engineering flattened (engineering_design_orientation, engineering_geometry_assessment)
- Array columns as comma-separated or JSON
```

#### Model 4b: `int_projects_phases` (Phase definitions)
One row per phase per project.
```
Columns:
- project_id
- phase_key
- phase_name
- phase_description
- phase_icon
- phase_order
- phase_category
- phase_statuses (as JSON)
- phase_steps (as JSON)
```

#### Model 4c: `int_projects_phase_steps` (Steps within phases)
One row per step per phase per project.
```
Columns:
- project_id
- phase_key
- step_key
- step_name
- step_order
- step_forms (as JSON)
```

#### Model 4d: `int_projects_phase_categories` (Phase categories)
One row per phase category per project.
```
Columns:
- project_id
- category_key
- category_name
- category_order
- only_listing
- flatten_steps
```

---

### 5. USERS (Priority 5)

**Row Count**: 162 users

**JSONB Columns**:
| Column | Type | Content |
|--------|------|---------|
| `phone` | Object | Phone info (mostly NULL) |
| `config` | Object | User config (languages, trainee_profile) |
| `picture` | Object | Profile picture (mostly NULL) |
| `linked_orgs` | Array | Linked organizations (mostly NULL) |
| `linked_projects` | Array | Linked projects (mostly NULL) |

**Proposed Model**:

#### Model 5a: `int_users_wide` (Main flat table)
```
Columns (~30):
- User identifiers (_id, uid, user_id, username)
- Basic info (name, email, user_type)
- Flags (is_deleted, is_superuser)
- Timestamps (created_at, updated_at, last_login)
- Phone flattened (phone_number, phone_country_code) -- if populated
- Config flattened (config_languages, config_trainee_profile)
- Picture flattened (picture_url) -- if populated
- Linked items kept as JSON (mostly NULL anyway)
```

---

## Implementation Plan

### Phase 1: Core Tables (Week 1)

| Day | Task | Model |
|-----|------|-------|
| 1 | Forms fields (DONE) | `int_forms_fields_wide` |
| 1-2 | Form records main | `int_form_records_wide` |
| 2-3 | Form records unpivoted | `int_form_records_data_unpivoted` |
| 3-4 | Form records with labels | `int_form_records_with_field_labels` |
| 4-5 | Users wide | `int_users_wide` |

### Phase 2: Project Tables (Week 2)

| Day | Task | Model |
|-----|------|-------|
| 1-2 | Project records main | `int_project_records_wide` |
| 2 | Project records steps | `int_project_records_steps` |
| 3 | Project records phases | `int_project_records_phases` |
| 3 | Project records data points | `int_project_records_data_points` |
| 4 | Project records status history | `int_project_records_status_history` |
| 4 | Project records linked forms | `int_project_records_linked_forms` |
| 5 | Projects main | `int_projects_wide` |
| 5 | Projects phases | `int_projects_phases` |

### Phase 3: Mart Layer (Week 3)

| Day | Task | Model |
|-----|------|-------|
| 1-2 | Per-form pivot tables | `mart_form_[form_name]` |
| 3 | Summary/aggregate tables | Various |
| 4-5 | Documentation & testing | Schema YAML |

---

## File Structure

```
models/
├── staging/
│   └── _sources.yml                    # Source definitions
│
├── intermediate/
│   ├── _sources.yml                    # Source definitions
│   ├── _int_models.yml                 # Schema & tests
│   │
│   ├── # Forms (DONE)
│   ├── int_forms_fields_wide.sql
│   │
│   ├── # Form Records
│   ├── int_form_records_wide.sql
│   ├── int_form_records_data_unpivoted.sql
│   ├── int_form_records_with_field_labels.sql
│   │
│   ├── # Project Records
│   ├── int_project_records_wide.sql
│   ├── int_project_records_steps.sql
│   ├── int_project_records_phases.sql
│   ├── int_project_records_data_points.sql
│   ├── int_project_records_status_history.sql
│   ├── int_project_records_linked_forms.sql
│   │
│   ├── # Projects
│   ├── int_projects_wide.sql
│   ├── int_projects_phases.sql
│   ├── int_projects_phase_steps.sql
│   ├── int_projects_phase_categories.sql
│   │
│   └── # Users
│       └── int_users_wide.sql
│
└── marts/
    ├── _mart_models.yml                # Schema & tests
    ├── mart_form_registration.sql      # Per-form pivot (example)
    ├── mart_form_enrollment.sql        # Per-form pivot (example)
    └── mart_summary_dashboard.sql      # Aggregated metrics
```

---

## Model Dependency Graph

```
                    staging.forms
                         │
                         ▼
              ┌─────────────────────┐
              │ int_forms_fields_wide│ (DONE)
              └─────────────────────┘
                         │
                         │ (provides field labels)
                         ▼
staging.form_records ───────────────────────────────────────┐
         │                                                  │
         ▼                                                  ▼
┌────────────────────────┐              ┌────────────────────────────────────┐
│ int_form_records_wide  │              │ int_form_records_data_unpivoted    │
└────────────────────────┘              └────────────────────────────────────┘
                                                    │
                                                    ▼
                                        ┌────────────────────────────────────┐
                                        │ int_form_records_with_field_labels │
                                        └────────────────────────────────────┘
                                                    │
                                                    ▼
                                        ┌────────────────────────────────────┐
                                        │ mart_form_[per_form_name]          │
                                        └────────────────────────────────────┘


staging.project_records ─────┬─────────────────────────────────────────┐
         │                   │                                         │
         ▼                   ▼                                         ▼
┌──────────────────┐  ┌────────────────────┐  ┌─────────────────────────────┐
│int_project_      │  │int_project_records_│  │int_project_records_         │
│records_wide      │  │steps               │  │data_points                  │
└──────────────────┘  └────────────────────┘  └─────────────────────────────┘
                             │
                             ▼
                      (+ phases, status_history, linked_forms)


staging.projects ────────────┬─────────────────┐
         │                   │                 │
         ▼                   ▼                 ▼
┌──────────────────┐  ┌──────────────┐  ┌─────────────────────┐
│int_projects_wide │  │int_projects_ │  │int_projects_phase_  │
│                  │  │phases        │  │steps                │
└──────────────────┘  └──────────────┘  └─────────────────────┘


staging.users
         │
         ▼
┌──────────────────┐
│ int_users_wide   │
└──────────────────┘
```

---

## Key Technical Patterns

### Pattern 1: Simple Object Flattening
```sql
-- Flatten a JSON object with known keys
SELECT
    json_column->>'key1' AS key1,
    json_column->>'key2' AS key2,
    (json_column->>'numeric_key')::numeric AS numeric_key
FROM table
```

### Pattern 2: Array Unpivoting (Long Format)
```sql
-- Convert JSON array to rows
SELECT
    id,
    elem.ordinality AS item_order,
    elem.value->>'key' AS item_key,
    elem.value->>'value' AS item_value
FROM table,
LATERAL jsonb_array_elements(json_array_column) WITH ORDINALITY AS elem(value, ordinality)
```

### Pattern 3: Array to Comma-Separated String
```sql
-- Convert JSON array to single string
SELECT
    (SELECT string_agg(elem::text, ', ') FROM jsonb_array_elements_text(json_array) AS elem) AS items_csv
FROM table
```

### Pattern 4: Dynamic Pivoting (Per-Form Tables)
```sql
-- Create pivot table for specific form
SELECT
    record_id,
    MAX(CASE WHEN field_key = 'country' THEN field_value END) AS country,
    MAX(CASE WHEN field_key = 'language' THEN field_value END) AS language,
    -- ... generated for each field
FROM int_form_records_data_unpivoted
WHERE form_id = 'specific_form_id'
GROUP BY record_id
```

### Pattern 5: Nested Array Flattening
```sql
-- Flatten nested arrays (e.g., phases -> steps)
SELECT
    project_id,
    phase.value->>'key' AS phase_key,
    step.value->>'key' AS step_key,
    step.value->>'name' AS step_name
FROM projects,
LATERAL jsonb_array_elements(phases) AS phase(value),
LATERAL jsonb_array_elements(phase.value->'steps') AS step(value)
```

---

## Benefits for Visualization

### Before (JSON columns):
```sql
-- Hard to query, BI tools can't use
SELECT data->>'country' FROM form_records  -- Manual JSON access
```

### After (Flat tables):
```sql
-- Easy filtering, grouping, joining
SELECT country, COUNT(*)
FROM mart_form_registration
GROUP BY country
```

### BI Tool Benefits:
1. **All columns visible** in dropdown menus
2. **Drag-and-drop** field selection
3. **No SQL knowledge** required for basic analysis
4. **Automatic charts** and aggregations
5. **Filter panels** work out-of-box
6. **Cross-table joins** are simple

---

## Estimated Deliverables

| Model | Estimated Columns | Estimated Rows |
|-------|-------------------|----------------|
| int_forms_fields_wide (DONE) | 119 | 256 |
| int_form_records_wide | ~50 | 6,478 |
| int_form_records_data_unpivoted | ~10 | ~200,000 |
| int_form_records_with_field_labels | ~15 | ~200,000 |
| int_project_records_wide | ~60 | 2,292 |
| int_project_records_steps | ~6 | ~5,000 |
| int_project_records_phases | ~6 | ~2,500 |
| int_project_records_data_points | ~8 | ~30,000 |
| int_project_records_status_history | ~5 | ~10,000 |
| int_project_records_linked_forms | ~7 | ~5,000 |
| int_projects_wide | ~80 | 2 |
| int_projects_phases | ~10 | ~20 |
| int_projects_phase_steps | ~8 | ~50 |
| int_projects_phase_categories | ~6 | ~6 |
| int_users_wide | ~30 | 162 |
| mart_form_[per_form] (9 forms) | varies | varies |

---

## Next Steps

1. [ ] **Review and approve this plan**
2. [ ] Start with `int_form_records_wide` (highest value)
3. [ ] Create `int_form_records_data_unpivoted` for dynamic data
4. [ ] Build remaining intermediate models
5. [ ] Add schema documentation and tests
6. [ ] Create mart layer for specific use cases

---

## Questions for Decision

1. **Materialization Strategy**:
   - `view` = always fresh, slower queries
   - `table` = faster queries, needs refresh
   - `incremental` = best for large tables with timestamps
   - **Recommendation**: `table` for all (data is small)

2. **Per-Form Pivot Tables**:
   - Should we create dedicated wide tables for each of the 9 forms?
   - **Recommendation**: Yes, creates best BI experience

3. **Historical Data**:
   - Should we track changes over time?
   - **Recommendation**: Not needed now, can add dbt snapshots later

4. **Naming Convention**:
   - Current: `int_[table]_[variant]`
   - Alternative: `stg_` for staging, `fct_` for facts, `dim_` for dimensions
   - **Recommendation**: Keep current simple naming
