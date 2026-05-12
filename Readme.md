# NEON HasStatic Availability – Network Intelligence Engine

## Project Overview

**NEON HasStatic Availability** is a **Parquet-first analytical platform** for diagnosing network site availability, infrastructure health, and operational risk across Vodafone's cellular network.

The system integrates multiple data sources (PAAS availability reports, R1 downtime analysis, RAN site temperature, rectifier/LVD diagnostics, generator status, and TX hub topology) into a unified analytical model, applies multi-domain risk scoring, and surfaces critical infrastructure issues through an interactive Streamlit dashboard.

---

## Architecture

### Data Flow

```
data/ (raw Excel)
  ↓
scripts/build_analytical_db.py (ETL + Parquet curate)
  ↓
parquet/ (analytical source of truth)
  ├── dimensions/        (reference tables: sites, hubs, master registry)
  ├── facts/             (fact tables: availability, downtime, temperature, rectifiers, generators)
  └── audit/             (data quality & lineage logs)
  ↓
scripts/queries.py (semantic queries + scoring)
  ↓
ui/dashboard.py (Streamlit presentation)
```

### Canonical Data Model

All sources normalize to a single **canonical site identifier**: `site_id`

**Source aliases for site_id:**

- `Site_Id` (PAAS, R1 reports)
- `SiteId` (RAN Temperature)
- `Atoll` / `Atoll_Id` / `atollid` (Generator, TX HubSite)
- `ATOLL_SITE_ID` (Master registry)

---

## Key Features

### 1. **Multi-Source ETL Pipeline**

The ETL (`build_analytical_db.py`) uses a registry-based pattern for extensibility:

- **Master SiteId Dimension** – Canonical site reference (geo, region, cluster metadata)
- **PAAS Availability** – R1 cell unavailability, power failures, backup capacity
- **R1 Downtime** – Incident summaries, downtime trends, voltage diagnostics
- **RAN Site Temperature** – Ambient & device temperature stress over 14 days
- **Rectifier/LVD** – Battery capacity, voltage warnings, module failures
- **Generator Status** – Fuel level, runtime hours, maintenance alerts
- **TX HubSite Topology** – Hub site relationships, child site risk propagation

Each source is configured via a single dictionary in `SOURCE_REGISTRY`:

- No changes needed to ETL logic to add new sources
- Automatic header/row handling, key normalization, Parquet output

### 2. **Risk Scoring Engine**

The query layer (`queries.py`) implements a **multi-domain scoring system** (0–100 per domain):

| Domain           | Weight | Signals                                                         |
| ---------------- | ------ | --------------------------------------------------------------- |
| **Availability** | 30%    | R1 unavailability %, power failures > 8hrs, missing AC/DC hours |
| **Downtime**     | 25%    | Downtime priority, total downtime hours, trend velocity         |
| **Temperature**  | 15%    | Extreme temp days (14d), high temp days, SFP module max temp    |
| **Rectifier**    | 15%    | Failed modules %, hours offline, LVD flag, battery capacity     |
| **Generator**    | 15%    | Fuel level, runtime hours, alarm state, maintenance overdue     |

**Priority Multiplier (Hub Blast Radius):**

- Each child site adds 5% uplift to hub's priority score
- Each at-risk child adds 8% (higher urgency)

### 3. **Semantic Query API**

`queries.py` exports high-level query functions for dashboard consumption:

- `get_site_scores()` – Full scored registry (all sites, composite priority)
- `get_signal_summary(domain)` – One domain's signal breakdown
- `get_power_crisis_sites()` – Generator + low fuel + power failures
- `get_thermal_risk_sites()` – Hot sites with concurrent power failures
- `get_infrastructure_sites()` – Rectifier decay + no safety net
- `get_lvd_battery_sites()` – LVD warnings + capacity below threshold
- `get_fuel_watch_sites()` – Generator sites sorted by fuel ascending
- `get_hub_drilldown(site_id)` – Hub + children detail view
- `get_region_snapshot()` – Top 20 worst sites by priority
- `load_history_trend(metric)` – Metric over time from snapshots

### 4. **Interactive Dashboard**

The Streamlit UI (`ui/dashboard.py`) provides:

- **6 Risk Views** – Power Crisis, Thermal Risk, Infrastructure Decay, LVD/Battery, Fuel Watch, Region Snapshot
- **Region Filters** – Drill down to specific regions
- **Metric Cards** – Live counts of at-risk sites per view
- **Sortable Tables** – Site details, components, scores per domain
- **Drilldown Detail** – Hub topology, child site risk cascade
- **JSON Export** – Site health scores & trending data
- **5-min Cache** – Query results cached for responsive UX

---

## Evolution & Development Phases

### Phase 1: Initial Proof-of-Concept (`v1` scripts)

**Files:** `build_analytical_db_v1.py`, `queries_v1.py`, `dashboard_v1.py`

- **Architecture:** CSV → DuckDB pattern
- **Scope:** 4 data sources (PAAS, Temperature, Generator, Power Failure)
- **Queries:** Basic joins & filters (static_generator_low_availability, availability_by_atoll)
- **Dashboard:** Sidebar atoll selector, simple bar chart
- **Limitations:**
  - No canonical key normalization
  - Hard-coded source paths & sheet names
  - Limited scoring (threshold-based only)
  - DuckDB analytics instead of Parquet-first model

### Phase 2: Production Architecture (`current` scripts)

**Files:** `build_analytical_db.py`, `queries.py`, `ui/dashboard.py`

**Major Improvements:**

| Aspect                  | v1                    | Current                               |
| ----------------------- | --------------------- | ------------------------------------- |
| **Storage**             | CSV + DuckDB          | Parquet (Polars)                      |
| **Extensibility**       | Hard-coded per source | Registry-based pattern                |
| **Key Normalization**   | None                  | Canonical `site_id` across 7+ aliases |
| **Data Sources**        | 4                     | 7 (+ placeholders for future)         |
| **Scoring**             | Threshold only        | 5-domain weighted system (0–100)      |
| **Queries**             | 2 basic queries       | 8+ semantic query functions           |
| **Priority Logic**      | N/A                   | Hub blast radius multiplier           |
| **Dashboard Views**     | 1 view                | 6 filtered risk views + drilldown     |
| **Caching**             | None                  | 5-min TTL per view                    |
| **Audit Trail**         | None                  | Audit logs in `parquet/audit/`        |
| **Historical Tracking** | None                  | Snapshots in `parquet/facts/history/` |

---

## Installation & Setup

### Prerequisites

- Python 3.10+
- Polars, Pandas, Streamlit
- openpyxl (Excel parsing)

### Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Place raw Excel in data/
# Expected files:
#   - PAAS_Availability_Weekly_Analysis_detail_*.xlsx
#   - R1_availability_Cause_Analysis_*.xlsx
#   - RAN_Site_Temperature_Analysis_*.xlsx
#   - RAN_Energy_Analysis_*.xlsx
#   - GeneratorSummary_*.xlsx
#   - TX_HubSite_Report_ZA*.xlsx
#   - Master_SiteId*.xlsx (canonical registry)

# 3. Build parquet analytical layer
python scripts/build_analytical_db.py

# 4. Launch dashboard
streamlit run ui/dashboard.py
```

---

## File Structure

```
.
├── data/
│   ├── DATA_SCHEMA.md                        # Input file specs & header rows
│   └── *.xlsx                                 # Raw source workbooks
├── parquet/
│   ├── dimensions/
│   │   ├── master_siteid.parquet             # Canonical site registry
│   │   ├── hubsite.parquet                   # TX hub topology
│   │   └── ...
│   ├── facts/
│   │   ├── availability_snapshot.parquet     # PAAS + R1 availability
│   │   ├── downtime_snapshot.parquet         # R1 downtime detail
│   │   ├── temperature_snapshot.parquet      # RAN site + SFP temp
│   │   ├── rectifier_snapshot.parquet        # Rectifier + LVD
│   │   ├── generator_snapshot.parquet        # Generator status
│   │   ├── site_scores.parquet               # Composite risk scores
│   │   └── history/                          # Time-series snapshots
│   └── audit/
│       └── load_*.json                       # ETL run logs
├── scripts/
│   ├── build_analytical_db.py                # ETL + Parquet build
│   ├── build_analytical_db_v1.py             # Legacy CSV/DuckDB version
│   ├── queries.py                            # Semantic query API
│   ├── queries_v1.py                         # Legacy basic queries
├── ui/
│   ├── dashboard.py                          # Streamlit dashboard
│   ├── dashboard_v1.py                       # Legacy single-view dashboard
├── pyproject.toml                            # Project metadata
├── requirements.txt                          # Python dependencies
├── Readme.md                                 # This file
└── .gitignore                                # Git exclusions

```

---

## Data Quality & Validation

The ETL pipeline includes:

- **Header row detection** – Each source specifies exact header row (1-indexed per Excel)
- **Type coercion** – Numeric fields cast with null-fill defaults
- **Normalization** – Site keys normalized to canonical `site_id`
- **Audit logging** – Load timestamps, source lineage, row counts
- **Snapshot versioning** – Each build creates timestamped snapshots

### Checking Data Integrity

```python
# After running build_analytical_db.py:
import polars as pl

# Verify canonical key cardinality
master = pl.read_parquet("parquet/dimensions/master_siteid.parquet")
print(f"Master site registry: {len(master)} unique sites")

# Check for orphaned records (no master_siteid match)
avail = pl.read_parquet("parquet/facts/availability_snapshot.parquet")
hub = pl.read_parquet("parquet/dimensions/hubsite.parquet")
print(f"Availability snapshot: {len(avail)} records")
print(f"HubSite hub registry: {len(hub)} hubs")
```

---

## Usage Examples

### Example 1: List Top 10 Infrastructure Risk Sites

```python
from scripts.queries import get_infrastructure_sites

sites = get_infrastructure_sites(region=None, top_n=10)
print(sites[["site_id", "site_name", "infrastructure_score"]].to_pandas())
```

### Example 2: Get Hub Drilldown (Children Risk Analysis)

```python
from scripts.queries import get_hub_drilldown

hub_data = get_hub_drilldown(site_id="SITE_123")
print(f"Hub: {hub_data['hub_name']}")
print(f"Children at risk: {hub_data['num_children_risk']}")
for child in hub_data["children"]:
    print(f"  - {child['site_name']}: {child['priority_score']:.1f}")
```

### Example 3: Export JSON Health Report

```python
from scripts.queries import export_json

json_path = export_json()
print(f"Health report exported to: {json_path}")
```

---

## Git Commit Strategy

### Commit 1: ETL Pipeline & Parquet Foundation

**Status:** ✅ Complete
**Files:** `scripts/build_analytical_db.py`, `parquet/`
**Changes:**

- Implement registry-based ETL pattern
- Add 7 source configurations (Master, PAAS, R1, Temperature, Rectifier, Generator, HubSite)
- Normalize all site keys to canonical `site_id`
- Write curated Parquet outputs to `dimensions/` and `facts/`
- Add audit logging & snapshot versioning

### Commit 2: Semantic Query API & Scoring Engine

**Status:** ✅ Complete
**Files:** `scripts/queries.py`
**Changes:**

- Implement 5-domain weighted scoring (availability, downtime, temperature, rectifier, generator)
- Add hub blast radius priority multiplier logic
- Create 8+ semantic query functions (get_site_scores, get_power_crisis_sites, etc.)
- Add historical trend loading from snapshots
- Implement JSON export for reporting

### Commit 3: Interactive Streamlit Dashboard

**Status:** ✅ Complete
**Files:** `ui/dashboard.py`
**Changes:**

- Build 6-view risk dashboard (Power Crisis, Thermal Risk, Infrastructure, LVD/Battery, Fuel Watch, Region Snapshot)
- Add region filter & top N controls
- Implement 5-min cache for query results
- Add metric cards, drilldown detail, sidebar navigation
- Integrate JSON health report visualization

### Commit 4: Data Schema & Documentation

**Status:** ✅ Complete
**Files:** `data/DATA_SCHEMA.md`, `Readme.md`, `Requirements.md`
**Changes:**

- Document input file specs (sheet names, header rows, key columns)
- Add canonical normalization rules & guardrails
- Create architecture decision record
- Export requirements.txt with all dependencies

### Commit 5: Git Repository Setup & Legacy Archive

**Status:** ✅ Complete
**Files:** `.gitignore`, `v1` scripts, `copilot_info.md`
**Changes:**

- Add `.gitignore` (ignore data/, parquet/, **pycache**)
- Archive v1 scripts as reference (build_analytical_db_v1.py, queries_v1.py, dashboard_v1.py)
- Add copilot instructions for workspace
- Initialize project metadata (pyproject.toml)

---

## Migration Notes: When to Keep v1 vs Current

### Use Current Scripts (`build_analytical_db.py`, `queries.py`, `dashboard.py`)

✅ **New work**  
✅ **Extension** (add new data sources)  
✅ **Scoring tuning** (adjust weights, thresholds)  
✅ **Dashboard development**

### Use v1 Scripts (Reference Only)

📖 **Understanding legacy approach**  
📖 **Comparing CSV/DuckDB vs. Parquet**  
📖 **Reverting to simpler prototype** (not recommended)

---

## Future Enhancements

- [ ] **Real-time streaming** – Replace daily snapshots with event-driven updates
- [ ] **Predictive scoring** – ML model for downtime risk forecasting
- [ ] **Custom rules engine** – User-defined alert thresholds per region
- [ ] **Power BI integration** – XMLA endpoint for enterprise BI tools
- [ ] **SLA tracking** – Automated SLA compliance reports & alerts
- [ ] **Feedback loop** – Incident resolution tracking to refine scoring weights

---

## Support & Troubleshooting

### Dashboard won't load

```bash
# Ensure ETL has run
python scripts/build_analytical_db.py

# Check Parquet outputs exist
ls -la parquet/facts/
ls -la parquet/dimensions/

# Restart dashboard
streamlit run ui/dashboard.py --logger.level=debug
```

### Missing data in dashboard

- Verify input Excel files are in `data/` with correct sheet names (see `DATA_SCHEMA.md`)
- Check audit logs: `parquet/audit/load_*.json`
- Review ETL output: `python scripts/build_analytical_db.py 2>&1 | grep -i error`

### Out of memory on large datasets

- Use Polars lazy evaluation in custom queries (`.scan_parquet()` instead of `.read_parquet()`)
- Increase heap: `python -Xmx8g scripts/build_analytical_db.py`

---

## License

Vodafone Network Intelligence Engine – Internal Use
