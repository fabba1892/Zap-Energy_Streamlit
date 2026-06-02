# Copilot Project Info - Neon HasStatic Availability

## What this project is

This project is a **Parquet-first analytical solution** for stitching together multiple network, availability, power, and environmental Excel reports into a reusable analytical model.

The solution goal is not only reporting, but also correlation and prioritisation of problematic sites, especially where infrastructure context and availability outcomes need to be analysed together.

---

## Target Architecture

```text
NEON_HASSTATIC_AVAILABILITY/
├─ data/                      # raw Excel files
│  ├─ DATA_SCHEMA.md
│  └─ *.xlsx
├─ parquet/
│  ├─ dimensions/
│  └─ facts/
├─ scripts/
│  ├─ build_analytical_db.py
│  └─ queries.py
├─ ui/
│  └─ dashboard.py
└─ .github/
   └─ copilot-instructions.md
```

### Layer responsibilities

- **data/** = raw source layer
- **build_analytical_db.py** = ingestion, normalization, and Parquet build layer
- **parquet/** = analytical source of truth
- **queries.py** = reusable semantic / business query layer
- **dashboard.py** = presentation layer

The project moved away from a CSV-first approach.
Parquet is now the intended storage standard for curated analytical outputs.

---

## Canonical Identifier Strategy

A key project rule is that multiple source field names represent the **same identifier concept**.
The naming differs because the reports were created by different analysts.

### Source aliases treated as the same identifier

- `Site_Id`
- `SiteId`
- `AtollId`
- `Atoll_Id`
- `Atoll`
- `atollid`

### Canonical curated name

Normalize all of these to:

```text
site_id
```

### Practical meaning

- PAAS `Site_Id`, R1 `Site_Id`, Temperature `SiteId`, GeneratorSummary `Atoll`, and Hubsite `atollid` are all treated as the same join-key concept for the model.
- The model should not assume a distinct `site -> atoll` relationship unless a future business rule explicitly proves that one is required.

---

## Source Workbook Contracts

### PAAS Availability Report

- workbook: `PAAS_Availability_Weekly_Analysis_detail_20260406.xlsx`
- sheet: `0_PaaS`
- first 2 rows: descriptive text / preamble
- row 3: headers
- row 4 onward: data
- source key field: `Site_Id`
- canonical key: `site_id`

### R1 Availability Cause Analysis

- workbook: `R1_availability_Cause_Analysis_20260409.xlsx`
- sheet: `0_Site Downtime Summary`
- first 2 rows: descriptive text / preamble
- row 3: headers
- row 4 onward: data
- source key field: `Site_Id`
- canonical key: `site_id`

### RAN Site Temperature Analysis

- workbook: `RAN_Site_Temperature_Analysis_20260409.xlsx`
- sheet: `0_SiteTemperature`
- first 2 rows: descriptive text / preamble
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
- active sheet: `Summary`
- workbook contains multiple sheets, but `Summary` is the current sheet in scope
- row 1: headers
- row 2 onward: data
- source key field: `Atoll`
- canonical key: `site_id`

### Tx Hub Site Power Failure Open Daily

- special-case source because it bridges through `CI Name`
- preferred bridge: `CI Name -> SiteName -> site_id`
- preferred matching source: PAAS
- secondary support source: Temperature report

---

## Current Curated Model Direction

### Core dimensions

- `parquet/dimensions/site.parquet`
- `parquet/dimensions/ci_to_site.parquet`

### Core facts

- `parquet/facts/availability_snapshot.parquet`
- `parquet/facts/temperature_snapshot.parquet`

### Transition artefact

Earlier drafts introduced `site_to_atoll.parquet` because the model originally assumed `SiteId` and `Atoll` were different entities.
That should now be treated as a temporary scaffold only, not as a required target-state concept.

---

## Development Guidance

### ETL guidance

- build from raw Excel in `data/`
- explicitly handle the right sheet and header row per workbook
- normalize key aliases to `site_id`
- write curated outputs to Parquet

### Query guidance

- read from Parquet only
- join on `site_id`
- keep business logic in `queries.py`
- add data-quality checks for unmapped records where useful

### UI guidance

- use `queries.py` outputs in Streamlit
- avoid dashboard-side raw joins and ETL logic

---

## Why this matters

The project is meant to stitch together multiple analyst-created reports that use inconsistent naming, but still refer to the same site identifier family.
The normalization rule is therefore one of the most important pieces of project context because it controls how the schema, ETL, and analytical joins should be designed.
