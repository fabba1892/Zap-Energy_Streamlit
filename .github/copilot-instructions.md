# Neon HasStatic Availability Analysis - Copilot Instructions

## Project Overview

This workspace builds a **Parquet-first analytical model** for network availability and infrastructure diagnostics.

The intended architecture is:

- raw Excel in `data/`
- curated Parquet in `parquet/`
- ETL / model build logic in `scripts/build_analytical_db.py`
- semantic analytics in `scripts/queries.py`
- presentation in `ui/dashboard.py`

This project has moved away from `csv_db/` as the primary architecture.
Do not suggest CSV as the default analytical persistence layer for this workspace.

---

## Canonical Identifier Normalization Rule

Treat the following source fields as aliases of the **same business identifier**:

- `Site_Id`
- `SiteId`
- `AtollId`
- `Atoll_Id`
- `Atoll`
- `atollid`

### Required normalization

Normalize all of them to the canonical curated key:

```text
site_id
```

### Guardrail

Do **not** assume that `Atoll` or `atollid` is a separate core entity unless the user explicitly introduces a distinct meaning later.
In this project those fields are currently treated as the same key concept used by different analysts.

---

## Excel Parsing Rules

Do not guess workbook structure.
Use the documented sheet and header rules below whenever generating ingestion logic.

### PAAS Availability Report

- workbook: `PAAS_Availability_Weekly_Analysis_detail_20260406.xlsx`
- sheet: `0_PAAS`
- rows 1-2: text / preamble
- row 3: headers
- row 4 onward: data
- source key field: `Site_Id`
- canonical key: `site_id`

### R1 Availability Cause Analysis

- workbook: `R1_availability_Cause_Analysis_20260409.xlsx`
- sheet: `0_Site Downtime Summary`
- rows 1-2: text / preamble
- row 3: headers
- row 4 onward: data
- source key field: `Site_Id`
- canonical key: `site_id`

### RAN Site Temperature Analysis

- workbook: `RAN_Site_Temperature_Analysis_20260409.xlsx`
- sheet: `0_SiteTemperature`
- rows 1-2: text / preamble
- row 3: headers
- row 4 onward: data
- source key field: `SiteId`
- canonical key: `site_id`

### TX HubSite Report ZA

- workbook: `TX_HubSite_Report_ZA (5).xlsx`
- sheet: `hubsite`
- row 1: headers
- row 2 onward: data
- source key field: `atollid`
- canonical key: `site_id`

### GeneratorSummary

- workbook: `GeneratorSummary_2026_03_27.xlsx`
- sheet: `Summary`
- note: workbook contains multiple sheets; `Summary` is the active modelling sheet unless expanded later
- row 1: headers
- row 2 onward: data
- source key field: `Atoll`
- canonical key: `site_id`

### Tx Hub Site Power Failure Open Daily

- use `CI Name` as the special-case bridge field
- preferred bridge: `CI Name -> SiteName -> site_id`
- preferred matching source: PAAS
- secondary support source: RAN Site Temperature Analysis

---

## Architecture Rules

### Raw layer

- Treat Excel files in `data/` as raw, immutable inputs
- Do not build dashboard logic directly on raw Excel workbooks

### Curated analytical layer

- Use Parquet as the analytical storage standard
- Write curated outputs to `parquet/dimensions/` and `parquet/facts/`
- Prefer Polars for ingestion, transformation, joins, and Parquet IO

### Semantic layer

- `scripts/queries.py` should read from Parquet only
- `scripts/queries.py` should implement business logic, joins, filters, and validation checks

### UI layer

- `ui/dashboard.py` should use query outputs
- `ui/dashboard.py` should stay focused on visualisation and interaction
- avoid embedding heavy transformation logic in Streamlit code

---

## Transition Note for Existing Scaffolding

Earlier project scaffolding and some current code still reference a separate `site_to_atoll` concept and legacy `csv_db` / DuckDB patterns.
Treat those as transition artefacts from the earlier design, not as the target architecture.

### Target-state guidance

- do not introduce a permanent conceptual `site -> atoll` bridge by default
- do not use `csv_db/` as the main modelling pattern
- prefer a unified `site_id`-based model
- if compatibility code exists temporarily, keep future suggestions aligned to the target normalized model

---

## When Generating or Updating Code

1. Read raw Excel from `data/`
2. Explicitly specify the correct sheet name
3. Explicitly handle the correct header row / skipped preamble rows
4. Normalize source key aliases to `site_id`
5. Write curated outputs to Parquet
6. Build reusable query functions against curated Parquet datasets
7. Keep Streamlit focused on presentation

---

## What Not To Suggest

Do not suggest the following unless the user explicitly asks for them:

- reintroducing `csv_db/` as the main analytical store
- modelling `Atoll` as a different core key by default
- assuming row 1 is always the header for every workbook
- querying raw Excel directly from the dashboard
- putting ETL logic directly into Streamlit UI code

---

## Default Mental Model

Use this mental model when assisting in the workspace:

- `data/` = raw source layer
- `build_analytical_db.py` = ingestion + normalization + Parquet build
- `parquet/` = analytical source of truth
- `queries.py` = business logic / semantic layer
- `dashboard.py` = presentation layer
- canonical key = `site_id`
