# requirements.md

## Project Requirements and Tech Stack

### Overview

This project is a **Parquet-first analytical application** for network availability and infrastructure analysis.
It ingests raw Excel reports, curates analytical datasets, and serves insights through a Streamlit dashboard.

---

## Current Tech Stack

### Core Language

- Python 3.11+ recommended

### Data Processing

- Polars
  - Used for Excel ingestion, transformation, joins, and Parquet read/write

### Storage Format

- Parquet
  - Used as the analytical persistence layer

### UI Layer

- Streamlit
  - Used to build the dashboard / presentation layer

### Filesystem / Utilities

- pathlib
- datetime

---

## Architecture Requirements

### 1. Raw Data Layer

- Source files are Excel workbooks stored in `data/`
- Raw files are treated as immutable inputs
- Raw source files should not be queried directly by the dashboard

### 2. Analytical Storage Layer

- Analytical outputs must be written to `parquet/`
- Dimension datasets must be stored under `parquet/dimensions/`
- Fact datasets must be stored under `parquet/facts/`
- Parquet is the single source of truth for analytical consumption

### 3. ETL / Build Layer

- `scripts/build_analytical_db.py` is responsible for:
  - reading raw Excel reports
  - transforming and standardising source data
  - generating dimensions and facts
  - writing Parquet outputs

### 4. Semantic Query Layer

- `scripts/queries.py` is responsible for:
  - reading Parquet datasets
  - applying joins and business logic
  - exposing reusable analytical functions
  - supporting validation and data-quality checks

### 5. Dashboard Layer

- `ui/dashboard.py` is responsible for:
  - calling query functions
  - rendering UI controls and visualisations
  - avoiding raw transformation logic where possible

---

## Required Python Packages

Install the following packages for the current architecture:

```bash
pip install polars openpyxl pyarrow streamlit
```

### Package Notes

- `polars` for dataframe operations and Parquet I/O
- `openpyxl` to support Excel workbook reading
- `pyarrow` for Parquet interoperability where required
- `streamlit` for the dashboard

---

## Optional Packages

These are not required for the Parquet-first baseline, but may be used later if intentionally adopted:

- duckdb
  - only if a SQL query layer is deliberately introduced later
- plotly
  - if richer interactive visualisations are needed in Streamlit
- pandas
  - avoid as a default dependency unless a specific edge case requires it

---

## Recommended Environment Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install polars openpyxl pyarrow streamlit
```

### Windows activation

```bash
.venv\Scripts\activate
```

---

## Expected Output Contracts

### Dimensions

- `parquet/dimensions/site.parquet`
- `parquet/dimensions/site_to_atoll.parquet`
- `parquet/dimensions/ci_to_site.parquet`

### Facts

- `parquet/facts/availability_snapshot.parquet`
- `parquet/facts/temperature_snapshot.parquet`

These names are the current baseline contract and can be extended over time.

---

## Non-Goals / Avoid

- Do not use CSV as the main analytical persistence layer
- Do not query raw Excel files directly from Streamlit
- Do not place heavy transformation logic in the UI layer
- Do not mix storage patterns without a deliberate design decision
