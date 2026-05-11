"""
NEON HasStatic Availability – ETL Pipeline
==========================================
Run:  python scripts/build_analytical_db.py
Reads raw Excel from data/, writes curated Parquet to parquet/

To add a new source in future: append one dict to SOURCE_REGISTRY.
No other code changes required.
"""

import re
import json
import logging
from datetime import date, datetime, time
from pathlib import Path

import polars as pl
from openpyxl import load_workbook

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT      = Path(__file__).parent.parent
DATA_DIR  = ROOT / "data"
PAR_DIR   = ROOT / "parquet"
DIMS_DIR  = PAR_DIR / "dimensions"
FACTS_DIR = PAR_DIR / "facts"
HIST_DIR  = FACTS_DIR / "history"
AUDIT_DIR = PAR_DIR / "audit"

for _d in [DIMS_DIR, FACTS_DIR, HIST_DIR, AUDIT_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("neon_etl")

# ── Canonical site-key aliases ────────────────────────────────────────────────
SITE_KEY_ALIASES = {"site_id", "siteid", "atollid", "atoll_id", "atoll", "atoll_id"}

# ── Source Registry ───────────────────────────────────────────────────────────
# Each dict fully describes one workbook source.
# Adding a new source = appending one new dict here.  Nothing else changes.
#
# Keys:
#   id            – unique source name
#   file_pattern  – glob matched against DATA_DIR (latest file wins if multiple)
#   sheet         – exact sheet name
#   header_row    – Excel row number of headers (1-indexed, matches DATA_SCHEMA.md)
#   site_key      – column name in the source that holds the site identifier
#   canonical_key – always "site_id"
#   keep_cols     – list of columns to extract (others discarded)
#   rename        – {source_col: curated_col}
#   fact_output   – filename stem written to parquet/facts/  (None = dimension)
#   dim_output    – filename stem written to parquet/dimensions/ (optional)
#   bridge        – special join strategy flag (optional)

SOURCE_REGISTRY = [
    # ── 0. Master SiteId — canonical site dimension anchor ───────────────────
    # Always loaded first. Every other source resolves against this.
    # CURRENT_IND = 'N' → site is inactive (kept but flagged, not excluded).
    {
        "id": "master_siteid",
        "file_pattern": "Master_SiteId*.xlsx",
        "sheet": "Master_SiteId",
        "header_row": 1,
        "site_key": "ATOLL_SITE_ID",
        "canonical_key": "site_id",
        "keep_cols": [
            "ATOLL_SITE_ID", "ATOLL_SITE_NAME", "COUNTRY", "REGION",
            "COUNTY_ID", "COUNTY", "LATITUDE", "LONGITUDE", "NETCON_HASL",
            "NED_METRO_CITY", "BS_NUMBER", "DISTRICT_COUNCIL",
            "REPLACEMENT_ATOLL_ID", "SERVICE_LEVEL", "CURRENT_IND",
            "ISHUBSITE", "ISVBSITE", "MUNICIPAL_DISTRICT", "URBAN_LAYER",
            "CBU_COUNTY_CLUSTER", "TOWNSHIP_ANE", "MRC_POLYGON",
            "LOCATION_KEY", "PARENT_LOCATION_KEY", "TOWN", "SUBURB", "CMN_NAME",
        ],
        "rename": {
            "ATOLL_SITE_ID":   "site_id",
            "ATOLL_SITE_NAME": "master_site_name",
            "REGION":          "region",
            "CURRENT_IND":     "is_active",        # 'Y' = active, 'N' = inactive
            "ISHUBSITE":       "is_hub",
            "ISVBSITE":        "is_vb_site",
            "LATITUDE":        "latitude",
            "LONGITUDE":       "longitude",
            "COUNTY":          "county",
            "COUNTY_ID":       "county_id",
            "TOWN":            "town",
            "SUBURB":          "suburb",
            "MUNICIPAL_DISTRICT": "municipal_district",
        },
        "fact_output": None,
        "dim_output": "master_siteid",   # → parquet/dimensions/master_siteid.parquet
    },

    # ── 1. PAAS Availability ─────────────────────────────────────────────────
    {
        "id": "paas",
        "file_pattern": "PAAS_Availability_Weekly_Analysis_detail_*.xlsx",
        "sheet": "0_PaaS",
        "header_row": 3,
        "site_key": "Site_Id",
        "canonical_key": "site_id",
        "keep_cols": [
            "Site_Id", "REGION", "Site Name", "Power", "Power Demand",
            "R1 Unavailability %",
            "R1 Unavailability (incl missing seconds) %",
            "Hrs Missing DC Power", "Hrs Missing AC Power",
            "Cells Down Ratio %", "Est backup time (hours)",
            "Rectifier Failed Modules", "Rectifier Modules",
            "Num Power Failures > 2Hrs", "Num Power Failures > 4Hrs",
            "Num Power Failures > 8Hrs",
            "Num Power Failures > 2Hrs (Last 30 Dy)",
            "Num Power Failures > 4Hrs (Last 30 Dy)",
            "Num Power Failures > 8Hrs (Last 30 Dy)",
            "GERM Battery capacity (AH)", "RAN Avg KW",
            "Load shedding stage active",
            "AC availability hours", "DC availability hours",
            "Unavailable AC %", "Unavailable DC %",
        ],
        "rename": {
            "Site_Id": "site_id",
            "Site Name": "site_name",
            "REGION": "region",
        },
        "fact_output": "availability_snapshot",
    },

    # ── 2. R1 Downtime Cause Analysis ────────────────────────────────────────
    {
        "id": "r1",
        "file_pattern": "R1_availability_Cause_Analysis_*.xlsx",
        "sheet": "0_Site Downtime Summary",
        "header_row": 3,
        "site_key": "Site_Id",
        "canonical_key": "site_id",
        "keep_cols": [
            "Site_Id", "REGION", "Site", "Max Tech",
            "Downtime Summary", "Downtime Analysis", "Downtime Priority",
            "Site Total Downtime Hrs",
            "Days - Site No DC Power > 1Hr (30 Days)",
            "Site Total No DC Power (30 Days)",
            "Days with 100% Cells Down > 1 Hr (30 Days)",
            "R1 Unavail % (Last 3Days)", "R1 Unavail % (Last 7Days)",
            "R1 Unavail % (Last 14Days)", "R1 Unavail % (Last Month)",
            "R1 Unavail % (Last 3Months)",
            "Cells Down", "Cells Down Ratio (%)",
            "Hrs Missing DC Power", "Hrs Missing AC Power",
            "Total Number of Incidents", "Number of Power Related Incidents",
            "Hub Site", "GZ Cluster", "Essential load config problem",
            "PS Traffic GB Lost due R1 (Coverage Loss) %",
            "Min Voltage 2Wk (V)", "Avg Max Voltage 2Wk (V)",
            "Voltage Swing 2Wk (V)",
        ],
        "rename": {
            "Site_Id": "site_id",
            "Site": "site_name",
            "REGION": "region",
        },
        "fact_output": "downtime_snapshot",
    },

    # ── 3. Site Temperature ───────────────────────────────────────────────────
    {
        "id": "temperature",
        "file_pattern": "RAN_Site_Temperature_Analysis_*.xlsx",
        "sheet": "0_SiteTemperature",
        "header_row": 3,
        "site_key": "SiteId",
        "canonical_key": "site_id",
        "keep_cols": [
            "SiteId", "REGION", "Site Name", "Warning",
            "Days - High Temp (14 Days)", "Days - Extreme Temp (14 Days)",
            "Weeks - High Temp (1 Year)", "Avg Temperature",
            "Avg Outside Ambient Temperature", "Max Outside Ambient Temperature",
            "Temperature Ranking", "Number of Temperature Incidents",
            "Temp Avg Huawei RAN (Board)", "Temp Max Huawei RAN",
            "Germ Battery Temp",
        ],
        "rename": {
            "SiteId": "site_id",
            "Site Name": "site_name",
            "REGION": "region",
        },
        "fact_output": "temperature_snapshot",
    },

    # ── 4. SFP / Radio Module Temperature ────────────────────────────────────
    # Uses MRBTS NAME as key → bridges to site_id via name normalisation
    # {
    #     "id": "sfp",
    #     "file_pattern": "RAN_Site_Temperature_Analysis_*.xlsx",
    #     "sheet": "0_SFP Temperature Analysis",
    #     "header_row": 3,
    #     "site_key": "MRBTS NAME",
    #     "canonical_key": "site_id",
    #     "bridge": "sfp_name_normalise",
    #     "keep_cols": [
    #         "REGION", "MRBTS_ID", "MRBTS NAME", "Temperature Warning",
    #         "RMod Temp Max", "RadioModule Temp Avg", "RadioModule Temp Max",
    #         "RadioModule Temp Max (Last 7 Days)",
    #         "RadioModule Temp Avg (Last 7 Days)",
    #         "RadioModule Temp Abv 60 (Last 7 Days)",
    #         "SysModule Temp (Last 7 Days)",
    #         "SysModule Temp Slope (Last 7 Days)",
    #         "SystemModule Temp Avg", "SystemModule Temp Max",
    #         "BaseModule Temp (Last 7 Days)",
    #         "BaseModule Temp Slope (Last 7 Days)",
    #     ],
    #     "rename": {"MRBTS NAME": "mrbts_name", "REGION": "region"},
    #     "fact_output": "sfp_temperature_snapshot",
    # },

    # ── 5. Rectifiers + LVD (RAN Energy Analysis) ────────────────────────────
    {
        "id": "rectifier",
        "file_pattern": "RAN_Energy_Analysis_*.xlsx",
        "sheet": "7_Rectifiers",
        "header_row": 3,
        "site_key": "ATOLL_ID",
        "canonical_key": "site_id",
        "keep_cols": [
            "ATOLL_ID", "Region", "sitename",
            "FailedModules", "FailedModules %", "NumberOfModules",
            "LVD Warning", "lvd", "pld", "batterycap", "batterycharge", "lvdreconnect",
            "System Power (KW)", "Load Power (KW)",
            "Hours on AC", "Hours on DC", "Hours Offline",
            "Days Rectifier No Connection", "Online Summary", "Warning",
        ],
        "rename": {
            "ATOLL_ID": "site_id",
            "Region": "region",
            "sitename": "site_name",
        },
        "fact_output": "rectifier_snapshot",
    },

    # ── 6. Generator Summary ──────────────────────────────────────────────────
    {
        "id": "generator",
        "file_pattern": "GeneratorSummary_*.xlsx",
        "sheet": "Summary",
        "header_row": 1,
        "site_key": "Atoll",
        "canonical_key": "site_id",
        "keep_cols": [
            "sitename", "Atoll", "devicename", "DeviceTypeName",
            "Days Last Communicated", "engine_rpm",
            "mains_l1n_voltage", "mains_l2n_voltage", "mains_l3n_voltage",
            "bat_voltage", "fuel_level", "RunTimeHours",
            "RunHours Till Maint", "alarmdesc", "FuelTank", "OutputPower",
        ],
        "rename": {"Atoll": "site_id", "sitename": "site_name"},
        "fact_output": "generator_snapshot",
    },

    # ── 7. TX HubSite Report ──────────────────────────────────────────────────
    # Goes to dimensions/ (not facts) – used to enrich site dimension + scoring
    {
        "id": "hubsite",
        "file_pattern": "TX_HubSite_Report_ZA*.xlsx",
        "sheet": "hubsite",
        "header_row": 1,
        "site_key": "atollid",
        "canonical_key": "site_id",
        "keep_cols": [
            "atollid", "locationname", "region",
            "numranlocations", "num_RAN_RISK_LOCATIONS",
            "num2G", "num3G", "num4G", "num5G",
            "numranservices", "numvbs",
            "list_OF_RAN_FAILED_LOCATIONS", "list_OF_RAN_RISK_LOCATIONS",
            "listofran", "huboverride", "delta",
        ],
        "rename": {"atollid": "site_id", "locationname": "hub_name"},
        "fact_output": None,
        "dim_output": "hubsite",
    },

    # ── FUTURE SOURCES ────────────────────────────────────────────────────────
    # To add: e.g. Power Failure Open Daily
    # {
    #     "id": "power_failure",
    #     "file_pattern": "Tx Hub Site_Power Failure_Open_Daily*.xlsx",
    #     "sheet": "<sheet name>",
    #     "header_row": <row>,
    #     "site_key": "CI Name",
    #     "canonical_key": "site_id",
    #     "bridge": "ci_name_to_site_id",
    #     "keep_cols": [...],
    #     "rename": {...},
    #     "fact_output": "power_failure_snapshot",
    # },
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalise_name(name: str | None) -> str:
    """Lowercase, strip, collapse non-alphanumeric to underscore."""
    if not name:
        return ""
    return re.sub(r"[^a-z0-9]+", "_", str(name).strip().lower()).strip("_")


def _resolve_file(pattern: str) -> Path | None:
    """Find latest matching file in DATA_DIR."""
    matches = sorted(DATA_DIR.glob(pattern))
    if not matches:
        log.warning("No file found for pattern: %s", pattern)
        return None
    if len(matches) > 1:
        log.info("Multiple files for '%s' – using latest: %s", pattern, matches[-1].name)
    return matches[-1]


def _is_numeric(v: str) -> bool:
    """Return True if string represents a finite number."""
    try:
        float(v)
        return True
    except (ValueError, TypeError):
        return False


def _read_sheet(file_path: Path, sheet: str, header_row: int) -> pl.DataFrame:
    """
    Read a sheet using openpyxl, honouring the exact header row from the schema.
    Returns a polars DataFrame with correct column names from header_row.
    """
    wb = load_workbook(file_path, read_only=True, data_only=True)
    ws = wb[sheet]

    all_rows = list(ws.iter_rows(min_row=header_row, values_only=True))
    wb.close()

    if len(all_rows) < 2:
        log.warning("%s | %s: no data rows found after header row %d", file_path.name, sheet, header_row)
        return pl.DataFrame()

    raw_headers = all_rows[0]
    data_rows   = all_rows[1:]

    # Deduplicate / clean header names
    seen: dict[str, int] = {}
    headers: list[str] = []
    for i, h in enumerate(raw_headers):
        name = str(h).strip() if h is not None else f"__col_{i}__"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        headers.append(name)

    # Build column dict (handle jagged rows gracefully)
    col_data: dict[str, list] = {h: [] for h in headers}
    for row in data_rows:
        for i, h in enumerate(headers):
            val = row[i] if i < len(row) else None
            # Convert datetime objects to string for polars compatibility
            if isinstance(val, (datetime, date, time)):
                val = val.isoformat()
            col_data[h].append(val)

    # Filter out completely empty rows
    n_cols = len(headers)
    non_empty = [
        i for i, _ in enumerate(data_rows)
        if any(col_data[h][i] not in (None, "", "None") for h in headers)
    ]
    if len(non_empty) < len(data_rows):
        log.info("  Dropped %d empty rows", len(data_rows) - len(non_empty))
        col_data = {h: [col_data[h][i] for i in non_empty] for h in headers}

    # Always build as strings first, then let polars cast — avoids mixed-type errors
    str_data = {h: [str(v) if v is not None else None for v in col_data[h]] for h in headers}
    try:
        df = pl.DataFrame(str_data)
        # Attempt numeric casting on likely numeric columns (non-destructive)
        cast_exprs = []
        for col in df.columns:
            sample_vals = [v for v in str_data[col] if v is not None][:20]
            numeric = all(
                _is_numeric(v) for v in sample_vals
            ) if sample_vals else False
            if numeric:
                cast_exprs.append(
                    pl.col(col).cast(pl.Float64, strict=False).alias(col)
                )
        if cast_exprs:
            df = df.with_columns(cast_exprs)
        return df
    except Exception as exc:
        log.error("DataFrame construction failed for %s/%s: %s", file_path.name, sheet, exc)
        return pl.DataFrame(str_data)


def _keep_and_rename(df: pl.DataFrame, keep_cols: list[str], rename: dict) -> pl.DataFrame:
    """Select only keep_cols that exist, rename per mapping."""
    available = [c for c in keep_cols if c in df.columns]
    missing   = [c for c in keep_cols if c not in df.columns]
    if missing:
        log.warning("  Missing columns (skipped): %s", missing)
    df = df.select(available)
    effective_rename = {k: v for k, v in rename.items() if k in df.columns}
    if effective_rename:
        df = df.rename(effective_rename)
    return df


def _normalise_site_id(df: pl.DataFrame, site_key_raw: str) -> pl.DataFrame:
    """Strip and uppercase the canonical site_id column."""
    if "site_id" in df.columns:
        df = df.with_columns(
            pl.col("site_id").cast(pl.Utf8).str.strip_chars().alias("site_id")
        )
    return df


# ── SFP Bridge ────────────────────────────────────────────────────────────────

def _build_sfp_bridge(
    sfp_df: pl.DataFrame,
    master_df: pl.DataFrame | None,
    paas_df: pl.DataFrame | None,
    temp_df: pl.DataFrame | None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Deterministic name-normalisation bridge:
      MRBTS NAME → normalised_key → site_id

    Lookup priority:
      1. Master_SiteId.ATOLL_SITE_NAME  (most complete, always available)
      2. PAAS.site_name                 (fallback)
      3. Temperature.site_name          (fallback)

    Unmatched rows → parquet/audit/sfp_unmatched.parquet
    """
    lookup: dict[str, str] = {}

    # Build lookup in reverse-priority order so highest-priority wins
    for ref_df, name_col in [
        (temp_df,   "site_name"),
        (paas_df,   "site_name"),
        (master_df, "master_site_name"),   # highest priority — applied last
    ]:
        if ref_df is None or ref_df.is_empty():
            continue
        sid_col = "site_id"
        if sid_col not in ref_df.columns or name_col not in ref_df.columns:
            continue
        for row in ref_df.select([sid_col, name_col]).to_dicts():
            key = _normalise_name(row.get(name_col, ""))
            if key and row.get(sid_col):
                lookup[key] = row[sid_col]

    log.info("  SFP bridge: %d name keys in lookup", len(lookup))

    results: list[dict] = []
    unmatched: list[dict] = []
    for row in sfp_df.to_dicts():
        norm = _normalise_name(row.get("mrbts_name", ""))
        site_id = lookup.get(norm)
        if site_id:
            results.append({**row, "site_id": site_id})
        else:
            unmatched.append({**row, "norm_key_attempted": norm})

    matched_pct = len(results) / max(len(sfp_df), 1) * 100
    log.info("  SFP bridge: %d/%d matched (%.1f%%)", len(results), len(sfp_df), matched_pct)

    out_df   = pl.DataFrame(results)   if results   else pl.DataFrame()
    audit_df = pl.DataFrame(unmatched) if unmatched else pl.DataFrame()
    return out_df, audit_df


# ── Site Dimension Builder ─────────────────────────────────────────────────────

def _build_site_dimension(source_frames: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """
    Build canonical site.parquet.

    Master_SiteId is the anchor — every known site starts here.
    Operational sources (PAAS, R1, temperature, etc.) contribute
    site_name and region only where Master_SiteId is null.
    Hubsite contributes topology columns (children, risk, tech counts).
    """
    master = source_frames.get("master_siteid")

    if master is not None and not master.is_empty():
        # Start from master — already has site_id, region, lat/long, county etc.
        site_dim = master.unique("site_id")
        log.info("  Master_SiteId anchor: %d sites", len(site_dim))

        # Operational sources add site_name where master has none
        for src_id in ["paas", "r1", "temperature", "rectifier", "generator"]:
            df = source_frames.get(src_id)
            if df is None or df.is_empty():
                continue
            cols = ["site_id"]
            if "site_name" in df.columns:
                cols.append("site_name")
            op = df.select(cols).unique("site_id")
            if "site_name" not in site_dim.columns:
                site_dim = site_dim.join(op, on="site_id", how="left")
            else:
                site_dim = site_dim.join(
                    op.rename({"site_name": "site_name_op"}),
                    on="site_id", how="left"
                ).with_columns(
                    pl.coalesce(["site_name", "site_name_op"]).alias("site_name")
                ).drop("site_name_op")
    else:
        # Graceful degradation — no master file, assemble from operational sources
        log.warning("  Master_SiteId not available — assembling dimension from operational sources")
        frames: list[pl.DataFrame] = []
        for src_id in ["paas", "r1", "temperature", "rectifier", "generator"]:
            df = source_frames.get(src_id)
            if df is None or df.is_empty():
                continue
            cols = ["site_id"]
            if "site_name" in df.columns: cols.append("site_name")
            if "region"    in df.columns: cols.append("region")
            frames.append(df.select(cols).unique("site_id"))

        if not frames:
            return pl.DataFrame()

        site_dim = frames[0]
        for frame in frames[1:]:
            site_dim = site_dim.join(frame, on="site_id", how="outer", suffix="_alt")
            for col in ["site_name", "region"]:
                alt = f"{col}_alt"
                if alt in site_dim.columns:
                    site_dim = site_dim.with_columns(
                        pl.coalesce([col, alt]).alias(col)
                    ).drop(alt)

    # ── Generator fallback bridge ─────────────────────────────────────────────
    # Generator uses sitename → no direct site_id in some rows.
    # Resolve via master_site_name → site_id lookup.
    gen_df = source_frames.get("generator")
    if gen_df is not None and not gen_df.is_empty():
        if "site_id" in gen_df.columns:
            null_gen = gen_df.filter(pl.col("site_id").is_null())
            if not null_gen.is_empty() and master is not None:
                log.info("  Generator fallback: resolving %d rows via master_site_name", len(null_gen))
                name_lookup = {
                    _normalise_name(r["master_site_name"]): r["site_id"]
                    for r in master.select(["site_id", "master_site_name"]).to_dicts()
                    if r.get("master_site_name")
                }
                resolved = []
                for row in null_gen.to_dicts():
                    norm = _normalise_name(row.get("site_name", ""))
                    sid  = name_lookup.get(norm)
                    if sid:
                        resolved.append({**row, "site_id": sid})
                if resolved:
                    fixed = pl.DataFrame(resolved)
                    source_frames["generator"] = pl.concat([
                        gen_df.filter(pl.col("site_id").is_not_null()),
                        fixed,
                    ])
                    log.info("  Generator fallback: resolved %d/%d rows", len(resolved), len(null_gen))

    # ── Hubsite topology ──────────────────────────────────────────────────────
    hub_df = source_frames.get("hubsite")
    if hub_df is not None and not hub_df.is_empty():
        hub_cols = ["site_id"] + [
            c for c in [
                "hub_name", "numranlocations", "num_RAN_RISK_LOCATIONS",
                "num2G", "num3G", "num4G", "num5G",
                "list_OF_RAN_FAILED_LOCATIONS", "list_OF_RAN_RISK_LOCATIONS",
            ] if c in hub_df.columns
        ]
        site_dim = site_dim.join(hub_df.select(hub_cols), on="site_id", how="left")

    # Cast numeric hub columns
    for col in ["numranlocations", "num_RAN_RISK_LOCATIONS",
                "num2G", "num3G", "num4G", "num5G"]:
        if col in site_dim.columns:
            site_dim = site_dim.with_columns(
                pl.col(col).cast(pl.Utf8).cast(pl.Int32, strict=False).fill_null(0)
            )

    # Cast lat/long to Float64 if present
    for col in ["latitude", "longitude"]:
        if col in site_dim.columns:
            site_dim = site_dim.with_columns(
                pl.col(col).cast(pl.Float64, strict=False)
            )

    return site_dim.unique("site_id")


# ── Main ETL ──────────────────────────────────────────────────────────────────

def run_etl(data_dir: Path = DATA_DIR) -> None:
    run_ts  = datetime.now()
    snap_ts = run_ts.strftime("%Y%m%d_%H%M%S")
    log.info("═" * 60)
    log.info("NEON ETL – run started %s", run_ts.isoformat())
    log.info("Data dir: %s", data_dir)
    log.info("═" * 60)

    source_frames: dict[str, pl.DataFrame] = {}

    # ── Ingest every registered source ────────────────────────────────────────
    for src in SOURCE_REGISTRY:
        sid = src["id"]
        log.info("── Source: %s ──", sid.upper())

        file_path = _resolve_file(src["file_pattern"])
        if file_path is None:
            log.warning("  SKIP – file not found")
            continue

        log.info("  File : %s", file_path.name)
        log.info("  Sheet: %s | Header row: %d", src["sheet"], src["header_row"])

        df = _read_sheet(file_path, src["sheet"], src["header_row"])
        if df.is_empty():
            log.warning("  SKIP – empty DataFrame after read")
            continue

        log.info("  Raw  : %d rows × %d cols", *df.shape)

        # Select + rename columns
        df = _keep_and_rename(df, src["keep_cols"], src["rename"])

        # Normalise the site_id column (non-bridge sources)
        if src.get("bridge") is None:
            src_key = src["site_key"]
            canonical = src.get("canonical_key", "site_id")
            # After rename the source key may already be "site_id"
            if canonical in df.columns:
                df = _normalise_site_id(df, canonical)
            elif src_key in df.columns:
                df = df.rename({src_key: canonical})
                df = _normalise_site_id(df, canonical)

        log.info("  Kept : %d rows × %d cols", *df.shape)
        source_frames[sid] = df

    # ── SFP Bridge ────────────────────────────────────────────────────────────
    if "sfp" in source_frames:
        log.info("── SFP Name Bridge ──")
        sfp_out, sfp_audit = _build_sfp_bridge(
            source_frames["sfp"],
            source_frames.get("master_siteid"),   # primary lookup
            source_frames.get("paas"),             # fallback 1
            source_frames.get("temperature"),      # fallback 2
        )
        source_frames["sfp"] = sfp_out
        if not sfp_audit.is_empty():
            audit_path = AUDIT_DIR / "sfp_unmatched.parquet"
            sfp_audit.write_parquet(audit_path)
            log.warning("  %d SFP rows unmatched → %s", len(sfp_audit), audit_path)

    # ── Site Dimension ────────────────────────────────────────────────────────
    log.info("── Building site dimension ──")
    site_dim = _build_site_dimension(source_frames)
    if not site_dim.is_empty():
        out = DIMS_DIR / "site.parquet"
        site_dim.write_parquet(out)
        log.info("  site.parquet: %d sites", len(site_dim))

    # ── Write Fact Parquets ───────────────────────────────────────────────────
    log.info("── Writing fact tables ──")
    written: list[str] = []
    for src in SOURCE_REGISTRY:
        sid = src["id"]
        fact_out = src.get("fact_output")
        dim_out  = src.get("dim_output")
        df = source_frames.get(sid)

        if df is None or df.is_empty():
            continue

        # Stamp every table with the snapshot timestamp
        df = df.with_columns(pl.lit(snap_ts).alias("snapshot_ts"))

        if fact_out:
            path = FACTS_DIR / f"{fact_out}.parquet"
            df.write_parquet(path)
            log.info("  %s.parquet: %d rows", fact_out, len(df))
            written.append(fact_out)

            # History snapshot
            hist = HIST_DIR / f"{fact_out}_{snap_ts}.parquet"
            df.write_parquet(hist)

        if dim_out:
            path = DIMS_DIR / f"{dim_out}.parquet"
            df.write_parquet(path)
            log.info("  dims/%s.parquet: %d rows", dim_out, len(df))

    # ── Run Manifest ──────────────────────────────────────────────────────────
    manifest = {
        "run_timestamp": run_ts.isoformat(),
        "snapshot_ts":   snap_ts,
        "sources_ingested": [
            {"id": s["id"], "rows": len(source_frames[s["id"]])}
            for s in SOURCE_REGISTRY
            if s["id"] in source_frames and not source_frames[s["id"]].is_empty()
        ],
        "facts_written": written,
        "site_count": len(site_dim) if not site_dim.is_empty() else 0,
    }
    manifest_path = PAR_DIR / "last_run.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    log.info("═" * 60)
    log.info("ETL complete – %d fact tables, %d sites", len(written), manifest["site_count"])
    log.info("Manifest: %s", manifest_path)


if __name__ == "__main__":
    run_etl()
