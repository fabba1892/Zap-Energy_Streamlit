"""
NEON HasStatic Availability – Query & Scoring Layer
====================================================
Reads from parquet/ only.  Never touches raw Excel.

Public API
----------
    get_site_scores()           → pl.DataFrame  full scored site table
    get_signal_summary(domain)  → pl.DataFrame  one domain's signals
    get_hub_drilldown(site_id)  → dict          hub + children detail
    export_json()               → Path          writes parquet/site_health.json
    load_history_trend(metric)  → pl.DataFrame  metric over time from snapshots
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import polars as pl

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT      = Path(__file__).parent.parent
PAR_DIR   = ROOT / "parquet"
DIMS_DIR  = PAR_DIR / "dimensions"
FACTS_DIR = PAR_DIR / "facts"
HIST_DIR  = FACTS_DIR / "history"

log = logging.getLogger("neon_queries")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

# ── Scoring weights (sum = 1.0) ───────────────────────────────────────────────
# Adjust these to tune priority sensitivity without changing any query logic.
SIGNAL_WEIGHTS = {
    "availability":  0.30,   # R1 unavailability, power failures
    "downtime":      0.25,   # total downtime hours, trend bands
    "temperature":   0.15,   # site + SFP module heat
    "rectifier":     0.15,   # failed modules, LVD, hours offline
    "generator":     0.15,   # fuel level, runtime, alarm state
}

# Blast radius multiplier constants (linear)
# priority_score = base_score × (1 + CHILD_WEIGHT × num_children)
#                              × (1 + RISK_WEIGHT  × num_risk_children)
CHILD_WEIGHT = 0.05   # Each child site adds 5% to the multiplier
RISK_WEIGHT  = 0.08   # Each at-risk child adds 8% (higher urgency)


# ── Loaders ───────────────────────────────────────────────────────────────────

def _load(name: str, subdir: Path = FACTS_DIR) -> Optional[pl.DataFrame]:
    path = subdir / f"{name}.parquet"
    if not path.exists():
        log.warning("Parquet not found: %s", path)
        return None
    return pl.read_parquet(path)


def _load_dim(name: str) -> Optional[pl.DataFrame]:
    return _load(name, DIMS_DIR)


# ── Signal Scorers (0–100 per domain) ─────────────────────────────────────────

def _score_availability(df: pl.DataFrame) -> pl.DataFrame:
    """
    Inputs (from availability_snapshot):
        R1 Unavailability %                          → 0=best, 100=worst  (weight 0.5)
        Num Power Failures > 8Hrs                    → 0=best, 20=max     (weight 0.3)
        Hrs Missing AC Power                         → 0=best, 168=max    (weight 0.2)
    """
    avail = df.select([
        "site_id",
        pl.col("R1 Unavailability %").cast(pl.Float64, strict=False).fill_null(0)
          .clip(0, 100).alias("r1_unavail_pct"),
        pl.col("Num Power Failures > 8Hrs").cast(pl.Float64, strict=False).fill_null(0)
          .clip(0, 20).truediv(20).mul(100).alias("pf_score"),
        pl.col("Hrs Missing AC Power").cast(pl.Float64, strict=False).fill_null(0)
          .clip(0, 168).truediv(168).mul(100).alias("ac_miss_score"),
    ]).with_columns(
        (
            pl.col("r1_unavail_pct")  * 0.5 +
            pl.col("pf_score")        * 0.3 +
            pl.col("ac_miss_score")   * 0.2
        ).clip(0, 100).alias("availability_score")
    ).select(["site_id", "availability_score", "r1_unavail_pct",
              "pf_score", "ac_miss_score"])
    return avail


def _score_downtime(df: pl.DataFrame) -> pl.DataFrame:
    """
    Inputs (from downtime_snapshot):
        Downtime Priority              → already 0–100 (weight 0.5)
        Site Total Downtime Hrs        → 0=best, 168=max (weight 0.3)
        R1 Unavail % (Last 7Days)      → 0=best, 100 (weight 0.2)
    """
    down = df.select([
        "site_id",
        pl.col("Downtime Priority").cast(pl.Float64, strict=False).fill_null(0)
          .clip(0, 100).alias("downtime_priority"),
        pl.col("Site Total Downtime Hrs").cast(pl.Float64, strict=False).fill_null(0)
          .clip(0, 168).truediv(168).mul(100).alias("downtime_hrs_score"),
        pl.col("R1 Unavail % (Last 7Days)").cast(pl.Float64, strict=False).fill_null(0)
          .clip(0, 100).alias("r1_7d_pct"),
    ]).with_columns(
        (
            pl.col("downtime_priority")   * 0.5 +
            pl.col("downtime_hrs_score")  * 0.3 +
            pl.col("r1_7d_pct")           * 0.2
        ).clip(0, 100).alias("downtime_score")
    ).select(["site_id", "downtime_score", "downtime_priority",
              "downtime_hrs_score", "r1_7d_pct"])
    return down


def _score_temperature(
    temp_df: pl.DataFrame,
    sfp_df: Optional[pl.DataFrame],
) -> pl.DataFrame:
    """
    Inputs:
        Days - Extreme Temp (14 Days)    → 0=best, 14=max  (weight 0.4)
        Days - High Temp (14 Days)       → 0=best, 14=max  (weight 0.3)
        RMod Temp Max (SFP)              → 0=best, 100°C   (weight 0.3)
    """
    temp = temp_df.select([
        "site_id",
        pl.col("Days - Extreme Temp (14 Days)").cast(pl.Float64, strict=False).fill_null(0)
          .clip(0, 14).truediv(14).mul(100).alias("extreme_temp_score"),
        pl.col("Days - High Temp (14 Days)").cast(pl.Float64, strict=False).fill_null(0)
          .clip(0, 14).truediv(14).mul(100).alias("high_temp_score"),
    ])

    # SFP: aggregate to site level (max radio module temp)
    if sfp_df is not None and not sfp_df.is_empty() and "site_id" in sfp_df.columns:
        sfp_agg = sfp_df.select([
            "site_id",
            pl.col("RMod Temp Max").cast(pl.Float64, strict=False).fill_null(0)
              .clip(0, 100).alias("rmod_temp_max"),
        ]).group_by("site_id").agg(pl.col("rmod_temp_max").max())
        temp = temp.join(sfp_agg, on="site_id", how="left")
        temp = temp.with_columns(pl.col("rmod_temp_max").fill_null(0))
    else:
        temp = temp.with_columns(pl.lit(0.0).alias("rmod_temp_max"))

    temp = temp.with_columns(
        (
            pl.col("extreme_temp_score")  * 0.4 +
            pl.col("high_temp_score")     * 0.3 +
            pl.col("rmod_temp_max")       * 0.3
        ).clip(0, 100).alias("temperature_score")
    )
    return temp.select(["site_id", "temperature_score", "extreme_temp_score",
                         "high_temp_score", "rmod_temp_max"])


def _score_rectifier(df: pl.DataFrame) -> pl.DataFrame:
    """
    Inputs (rectifier_snapshot):
        FailedModules %    → 0=best, 100  (weight 0.4)
        Hours Offline      → 0=best, 168  (weight 0.3)
        lvd                → 0/1 flag     (weight 0.2)
        batterycap         → low = bad    (weight 0.1)
    Aggregated to site_id level (max of worst module).
    """
    rect = df.select([
        "site_id",
        pl.col("FailedModules %").cast(pl.Float64, strict=False).fill_null(0)
          .clip(0, 100).alias("failed_modules_pct"),
        pl.col("Hours Offline").cast(pl.Float64, strict=False).fill_null(0)
          .clip(0, 168).truediv(168).mul(100).alias("offline_score"),
        pl.col("lvd").cast(pl.Float64, strict=False).fill_null(0)
          .clip(0, 1).mul(100).alias("lvd_score"),
        pl.col("batterycap").cast(pl.Float64, strict=False).fill_null(100)
          .sub(100).abs().clip(0, 100).alias("batt_cap_score"),
    ]).group_by("site_id").agg([
        pl.col("failed_modules_pct").max(),
        pl.col("offline_score").max(),
        pl.col("lvd_score").max(),
        pl.col("batt_cap_score").max(),
    ]).with_columns(
        (
            pl.col("failed_modules_pct") * 0.4 +
            pl.col("offline_score")      * 0.3 +
            pl.col("lvd_score")          * 0.2 +
            pl.col("batt_cap_score")     * 0.1
        ).clip(0, 100).alias("rectifier_score")
    )
    return rect.select(["site_id", "rectifier_score", "failed_modules_pct",
                         "offline_score", "lvd_score"])


def _score_generator(df: pl.DataFrame) -> pl.DataFrame:
    """
    Inputs (generator_snapshot):
        fuel_level         → low = worse  (weight 0.5)  [0–100%]
        alarmdesc          → GEN RUNNING = risk flag     (weight 0.3)
        RunHours Till Maint→ low = closer to maintenance (weight 0.2)
    fuel_level = -1 means comms lost → treat as 0% fuel.
    """
    gen = df.select([
        "site_id",
        # clip(0.0, 100.0) already handles negatives — no lambda needed
        pl.col("fuel_level").cast(pl.Float64, strict=False)
          .fill_null(0.0).clip(0.0, 100.0).alias("fuel_level_pct"),
        pl.col("alarmdesc").cast(pl.Utf8).fill_null("").alias("alarmdesc"),
        # fill_null MUST be float literal; clip(0.0, 500.0) replaces max(v,0) lambda
        pl.col("RunHours Till Maint").cast(pl.Float64, strict=False)
          .fill_null(500.0).clip(0.0, 500.0).alias("run_hrs_till_maint"),
    ]).with_columns([
        # Low fuel = high score (bad)
        (100.0 - pl.col("fuel_level_pct")).alias("fuel_risk"),
        # Running alarm = 100 risk, other = 0
        pl.col("alarmdesc").str.to_uppercase()
          .str.contains("RUNNING|ALARM|FAULT|LOST")
          .cast(pl.Int32).mul(100).alias("alarm_score"),
        # Low hours till maintenance = higher score
        (1.0 - pl.col("run_hrs_till_maint") / 500.0).mul(100).clip(0, 100)
          .alias("maint_score"),
    ]).group_by("site_id").agg([
        pl.col("fuel_risk").max(),
        pl.col("alarm_score").max(),
        pl.col("maint_score").max(),
        pl.col("fuel_level_pct").min().alias("fuel_level_pct"),
        pl.col("alarmdesc").first(),
    ]).with_columns(
        (
            pl.col("fuel_risk")    * 0.5 +
            pl.col("alarm_score")  * 0.3 +
            pl.col("maint_score")  * 0.2
        ).clip(0, 100).alias("generator_score")
    )
    return gen.select(["site_id", "generator_score", "fuel_level_pct",
                        "alarm_score", "alarmdesc"])


# ── Blast Radius Multiplier ────────────────────────────────────────────────────

def _blast_radius_multiplier(site_dim: pl.DataFrame) -> pl.DataFrame:
    """
    Linear multiplier based on hub topology:
        multiplier = 1 + (CHILD_WEIGHT × num_children)
                       + (RISK_WEIGHT  × num_risk_children)
    Standalone sites (no hubsite row) → multiplier = 1.0
    """
    hub_cols = ["site_id"]
    for col in ["numranlocations", "num_RAN_RISK_LOCATIONS"]:
        if col in site_dim.columns:
            hub_cols.append(col)

    br = site_dim.select(hub_cols)

    if "numranlocations" not in br.columns:
        br = br.with_columns(pl.lit(0).alias("numranlocations"))
    if "num_RAN_RISK_LOCATIONS" not in br.columns:
        br = br.with_columns(pl.lit(0).alias("num_RAN_RISK_LOCATIONS"))

    br = br.with_columns([
        pl.col("numranlocations").cast(pl.Float64).fill_null(0).alias("n_children"),
        pl.col("num_RAN_RISK_LOCATIONS").cast(pl.Float64).fill_null(0).alias("n_risk"),
    ]).with_columns(
        (
            1.0
            + pl.col("n_children") * CHILD_WEIGHT
            + pl.col("n_risk")     * RISK_WEIGHT
        ).alias("blast_multiplier")
    )
    return br.select(["site_id", "blast_multiplier", "n_children", "n_risk"])


# ── Master Score Table ─────────────────────────────────────────────────────────

def get_site_scores() -> pl.DataFrame:
    """
    Build the full scored site table.
    Every site_id from the dimension gets a row; missing signals default to 0.

    Columns returned include:
        site_id, site_name, region,
        availability_score, downtime_score, temperature_score,
        rectifier_score, generator_score,
        base_score, blast_multiplier, priority_score,
        n_children, n_risk,
        + key signal columns from each domain
    """
    site_dim   = _load_dim("site")
    avail_df   = _load("availability_snapshot")
    down_df    = _load("downtime_snapshot")
    temp_df    = _load("temperature_snapshot")
    sfp_df     = _load("sfp_temperature_snapshot")
    rect_df    = _load("rectifier_snapshot")
    gen_df     = _load("generator_snapshot")

    if site_dim is None:
        log.error("site.parquet missing – run build_analytical_db.py first")
        return pl.DataFrame()

    scored = site_dim.select(
        [c for c in ["site_id", "site_name", "region",
                     "numranlocations", "num_RAN_RISK_LOCATIONS",
                     "num2G", "num3G", "num4G", "num5G",
                     "list_OF_RAN_FAILED_LOCATIONS",
                     "list_OF_RAN_RISK_LOCATIONS"]
         if c in site_dim.columns]
    )

    # ── Join each domain score ─────────────────────────────────────────────────
    domain_scores: list[tuple[str, pl.DataFrame]] = []

    if avail_df is not None:
        domain_scores.append(("availability", _score_availability(avail_df)))
    if down_df is not None:
        domain_scores.append(("downtime", _score_downtime(down_df)))
    if temp_df is not None:
        domain_scores.append(("temperature", _score_temperature(temp_df, sfp_df)))
    if rect_df is not None:
        domain_scores.append(("rectifier", _score_rectifier(rect_df)))
    if gen_df is not None:
        domain_scores.append(("generator", _score_generator(gen_df)))

    for domain, df in domain_scores:
        scored = scored.join(df, on="site_id", how="left")

    # Fill missing domain scores with 0
    score_cols = [f"{d}_score" for d in SIGNAL_WEIGHTS]
    for col in score_cols:
        if col not in scored.columns:
            scored = scored.with_columns(pl.lit(0.0).alias(col))
        else:
            scored = scored.with_columns(
                pl.col(col).fill_null(0.0)
            )

    # ── Base score: weighted sum ───────────────────────────────────────────────
    scored = scored.with_columns(
        pl.sum_horizontal(
            pl.col(f"{domain}_score") * weight
            for domain, weight in SIGNAL_WEIGHTS.items()
        ).clip(0, 100).alias("base_score")
    )

    # ── Blast radius multiplier ────────────────────────────────────────────────
    br = _blast_radius_multiplier(site_dim)
    scored = scored.join(br.select(["site_id", "blast_multiplier",
                                     "n_children", "n_risk"]),
                         on="site_id", how="left")
    scored = scored.with_columns(
        pl.col("blast_multiplier").fill_null(1.0)
    )

    # ── Priority score ────────────────────────────────────────────────────────
    scored = scored.with_columns(
        (pl.col("base_score") * pl.col("blast_multiplier"))
        .clip(0, 200)          # cap at 200 so multiplied scores stay legible
        .round(2)
        .alias("priority_score")
    )

    # ── Severity band ─────────────────────────────────────────────────────────
    scored = scored.with_columns(
        pl.when(pl.col("priority_score") >= 80).then(pl.lit("CRITICAL"))
          .when(pl.col("priority_score") >= 55).then(pl.lit("HIGH"))
          .when(pl.col("priority_score") >= 30).then(pl.lit("MEDIUM"))
          .otherwise(pl.lit("LOW"))
          .alias("severity")
    )

    return scored.sort("priority_score", descending=True)


# ── Signal Summary (per domain) ────────────────────────────────────────────────

def get_signal_summary(domain: str) -> Optional[pl.DataFrame]:
    """Return the raw fact table for a single domain, joined with site metadata."""
    fact_map = {
        "availability":  "availability_snapshot",
        "downtime":      "downtime_snapshot",
        "temperature":   "temperature_snapshot",
        "sfp":           "sfp_temperature_snapshot",
        "rectifier":     "rectifier_snapshot",
        "generator":     "generator_snapshot",
    }
    fact_name = fact_map.get(domain)
    if not fact_name:
        log.warning("Unknown domain: %s. Valid: %s", domain, list(fact_map))
        return None

    df = _load(fact_name)
    if df is None:
        return None

    site_dim = _load_dim("site")
    if site_dim is not None:
        meta_cols = [c for c in ["site_id", "site_name", "region"] if c in site_dim.columns]
        df = df.join(site_dim.select(meta_cols), on="site_id", how="left")

    return df


# ── Hub Drilldown ──────────────────────────────────────────────────────────────

def get_hub_drilldown(site_id: str) -> dict:
    """
    Return hub detail for a given site_id:
        - hub metadata (children count, risk count, tech breakdown)
        - list of failed child locations
        - list of at-risk child locations
        - priority score for this site
    """
    site_dim = _load_dim("site")
    if site_dim is None:
        return {"error": "site dimension not built"}

    row = site_dim.filter(pl.col("site_id") == site_id)
    if row.is_empty():
        return {"error": f"site_id '{site_id}' not found"}

    scores = get_site_scores()
    score_row = scores.filter(pl.col("site_id") == site_id)

    result = row.to_dicts()[0]
    if not score_row.is_empty():
        s = score_row.to_dicts()[0]
        result["priority_score"]    = s.get("priority_score")
        result["severity"]          = s.get("severity")
        result["base_score"]        = s.get("base_score")
        result["blast_multiplier"]  = s.get("blast_multiplier")
        result["availability_score"] = s.get("availability_score")
        result["generator_score"]    = s.get("generator_score")
        result["rectifier_score"]    = s.get("rectifier_score")
        result["temperature_score"]  = s.get("temperature_score")

    return result


# ── History Trend ──────────────────────────────────────────────────────────────

def load_history_trend(
    fact_name: str,
    metric_col: str,
    site_ids: Optional[list[str]] = None,
    agg: str = "mean",
) -> pl.DataFrame:
    """
    Scan all history snapshots for fact_name, return metric_col over time.

    Args:
        fact_name  – e.g. "availability_snapshot"
        metric_col – e.g. "R1 Unavailability %"
        site_ids   – filter to specific sites (None = all)
        agg        – "mean" | "max" | "min" aggregation across sites per snapshot

    Returns DataFrame with columns: [snapshot_ts, metric_col]
    """
    pattern = str(HIST_DIR / f"{fact_name}_*.parquet")
    try:
        lf = pl.scan_parquet(pattern)
    except Exception as exc:
        log.warning("No history found for %s: %s", fact_name, exc)
        return pl.DataFrame()

    if site_ids:
        lf = lf.filter(pl.col("site_id").is_in(site_ids))

    agg_expr = {
        "mean": pl.col(metric_col).cast(pl.Float64, strict=False).mean(),
        "max":  pl.col(metric_col).cast(pl.Float64, strict=False).max(),
        "min":  pl.col(metric_col).cast(pl.Float64, strict=False).min(),
    }.get(agg, pl.col(metric_col).cast(pl.Float64, strict=False).mean())

    try:
        result = (
            lf.group_by("snapshot_ts")
              .agg(agg_expr.alias(metric_col))
              .sort("snapshot_ts")
              .collect()
        )
    except Exception as exc:
        log.error("Trend query failed: %s", exc)
        return pl.DataFrame()

    return result


# ── JSON Export ───────────────────────────────────────────────────────────────

def export_json(out_path: Optional[Path] = None) -> Path:
    """
    Write parquet/site_health.json – a pre-aggregated portable payload
    consumable by Streamlit, HTML/JS dashboards, or any downstream tool.
    """
    if out_path is None:
        out_path = PAR_DIR / "site_health.json"

    scores = get_site_scores()
    if scores.is_empty():
        log.error("No scores generated – skipping JSON export")
        return out_path

    # ── Summary block ─────────────────────────────────────────────────────────
    severity_counts = (
        scores.group_by("severity")
              .agg(pl.len().alias("count"))
              .to_dicts()
    )
    summary = {
        "total_sites":    len(scores),
        "critical":       next((r["count"] for r in severity_counts if r["severity"] == "CRITICAL"), 0),
        "high":           next((r["count"] for r in severity_counts if r["severity"] == "HIGH"), 0),
        "medium":         next((r["count"] for r in severity_counts if r["severity"] == "MEDIUM"), 0),
        "low":            next((r["count"] for r in severity_counts if r["severity"] == "LOW"), 0),
        "avg_priority":   round(float(scores.select(pl.col("priority_score").mean()).item() or 0), 2),
    }

    # ── Per-site payload ──────────────────────────────────────────────────────
    site_rows: list[dict] = []
    for row in scores.to_dicts():
        site = {
            "site_id":        row.get("site_id"),
            "site_name":      row.get("site_name"),
            "region":         row.get("region"),
            "priority_score": row.get("priority_score"),
            "severity":       row.get("severity"),
            "base_score":     row.get("base_score"),
            "blast_multiplier": row.get("blast_multiplier"),
            "n_children":     int(row.get("n_children") or 0),
            "n_risk":         int(row.get("n_risk") or 0),
            "failed_locations": row.get("list_OF_RAN_FAILED_LOCATIONS"),
            "risk_locations":   row.get("list_OF_RAN_RISK_LOCATIONS"),
            "tech": {
                "2G": int(row.get("num2G") or 0),
                "3G": int(row.get("num3G") or 0),
                "4G": int(row.get("num4G") or 0),
                "5G": int(row.get("num5G") or 0),
            },
            "signals": {
                "availability":  round(row.get("availability_score") or 0, 1),
                "downtime":      round(row.get("downtime_score") or 0, 1),
                "temperature":   round(row.get("temperature_score") or 0, 1),
                "rectifier":     round(row.get("rectifier_score") or 0, 1),
                "generator":     round(row.get("generator_score") or 0, 1),
            },
        }
        site_rows.append(site)

    payload = {
        "generated_at":  datetime.now().isoformat(),
        "score_weights": SIGNAL_WEIGHTS,
        "blast_params":  {"child_weight": CHILD_WEIGHT, "risk_weight": RISK_WEIGHT},
        "summary":       summary,
        "sites":         site_rows,
    }

    out_path.write_text(json.dumps(payload, indent=2, default=str))
    log.info("JSON export → %s (%d sites)", out_path, len(site_rows))
    return out_path


# ── Cross-Signal View Queries ─────────────────────────────────────────────────
# Each function returns a display-ready DataFrame for one dashboard view.
# Columns are human-readable. Status cells use emoji + label + value format.
# region=None or "All" → return all regions.

def _site_meta() -> pl.DataFrame:
    """Minimal site metadata for enriching view tables."""
    dim = _load_dim("site")
    if dim is None:
        return pl.DataFrame({"site_id": [], "site_name": [], "region": [],
                             "n_children": [], "n_risk": []})
    cols = [c for c in ["site_id", "site_name", "region",
                         "numranlocations", "num_RAN_RISK_LOCATIONS",
                         "county", "is_hub"] if c in dim.columns]
    meta = dim.select(cols)
    if "numranlocations" in meta.columns:
        meta = meta.rename({"numranlocations": "n_children"})
    if "num_RAN_RISK_LOCATIONS" in meta.columns:
        meta = meta.rename({"num_RAN_RISK_LOCATIONS": "n_risk"})
    for col in ["n_children", "n_risk"]:
        if col in meta.columns:
            meta = meta.with_columns(
                pl.col(col).cast(pl.Int32, strict=False).fill_null(0)
            )
    return meta


def _status(value: pl.Expr, thresholds: tuple, labels: tuple = ("CRITICAL","WARNING","OK"),
            fmt: str = "{:.1f}") -> pl.Expr:
    """
    Build a single status string: '🔴 CRITICAL — 12.3%'
    thresholds = (critical_threshold, warning_threshold) — values ABOVE = worse
    """
    crit, warn = thresholds
    fmt_expr = value.cast(pl.Float64, strict=False).round(1).cast(pl.Utf8)
    return (
        pl.when(value.cast(pl.Float64, strict=False) >= crit)
          .then(pl.concat_str([pl.lit(f"🔴 {labels[0]} — "), fmt_expr]))
          .when(value.cast(pl.Float64, strict=False) >= warn)
          .then(pl.concat_str([pl.lit(f"🟠 {labels[1]} — "), fmt_expr]))
          .otherwise(pl.concat_str([pl.lit(f"🟢 {labels[2]} — "), fmt_expr]))
    )


def _fuel_status(value: pl.Expr) -> pl.Expr:
    """Fuel is inverse — LOW is bad."""
    fmt_expr = value.cast(pl.Float64, strict=False).round(1).cast(pl.Utf8)
    return (
        pl.when(value.cast(pl.Float64, strict=False) <= 15)
          .then(pl.concat_str([pl.lit("🔴 CRITICAL — "), fmt_expr, pl.lit("%")]))
          .when(value.cast(pl.Float64, strict=False) <= 30)
          .then(pl.concat_str([pl.lit("🟠 WARNING — "), fmt_expr, pl.lit("%")]))
          .otherwise(pl.concat_str([pl.lit("🟢 OK — "), fmt_expr, pl.lit("%")]))
    )


def _filter_region(df: pl.DataFrame, region: str | None) -> pl.DataFrame:
    if region and region != "All" and "region" in df.columns:
        return df.filter(pl.col("region") == region)
    return df


def _top_n(df: pl.DataFrame, sort_col: str, n: int = 20,
           descending: bool = True) -> pl.DataFrame:
    if sort_col not in df.columns:
        return df.head(n)
    return df.sort(sort_col, descending=descending).head(n)


# ── View 1: Power Crisis ──────────────────────────────────────────────────────

def get_power_crisis_sites(region: str | None = None, top_n: int = 20) -> pl.DataFrame:
    """
    Sites with generator issues AND availability problems.
    Worst = running on generator with low fuel and repeated power failures.
    """
    gen_df  = _load("generator_snapshot")
    avail_df = _load("availability_snapshot")
    meta    = _site_meta()

    if gen_df is None:
        return pl.DataFrame()

    # Generator columns
    gen = gen_df.select([c for c in [
        "site_id", "fuel_level", "alarmdesc", "RunHours Till Maint",
        "RunTimeHours", "FuelTank", "Days Last Communicated",
    ] if c in gen_df.columns]).with_columns([
        pl.col("fuel_level").cast(pl.Float64, strict=False).fill_null(0.0)
          .clip(0.0, 100.0).alias("fuel_raw"),
        pl.col("alarmdesc").cast(pl.Utf8).fill_null("UNKNOWN").alias("alarm_raw"),
    ])

    # Availability columns
    if avail_df is not None:
        avail = avail_df.select([c for c in [
            "site_id",
            "Num Power Failures > 8Hrs",
            "Num Power Failures > 4Hrs",
            "Hrs Missing DC Power",
            "R1 Unavailability %",
        ] if c in avail_df.columns])
        gen = gen.join(avail, on="site_id", how="left")

    # Enrich with site meta
    gen = gen.join(meta, on="site_id", how="left")
    gen = _filter_region(gen, region)

    # Build display columns
    pf_col = "Num Power Failures > 8Hrs" if "Num Power Failures > 8Hrs" in gen.columns else None
    r1_col = "R1 Unavailability %" if "R1 Unavailability %" in gen.columns else None
    dc_col = "Hrs Missing DC Power" if "Hrs Missing DC Power" in gen.columns else None

    display = gen.with_columns([
        pl.col("site_name").fill_null(pl.col("site_id")).alias("Site"),
        pl.col("region").fill_null("—").alias("Region"),
        _fuel_status(pl.col("fuel_raw")).alias("Fuel Level"),
        pl.col("alarm_raw").alias("Generator Alarm"),
        (pl.col("n_children").cast(pl.Utf8) + pl.lit(" sites")).alias("Children"),
        (pl.col("n_risk").cast(pl.Utf8) + pl.lit(" at risk")).alias("At Risk"),
    ])

    if pf_col:
        display = display.with_columns(
            _status(pl.col(pf_col), (5, 2), ("HIGH", "MEDIUM", "LOW"))
            .alias("Power Failures >8h")
        )
    if r1_col:
        display = display.with_columns(
            _status(pl.col(r1_col), (5.0, 1.0)).alias("R1 Unavail %")
        )

    # Filter: only sites with actual generator or availability problems
    problem_filter = pl.col("fuel_raw") <= 30
    if pf_col:
        problem_filter = problem_filter | (
            pl.col(pf_col).cast(pl.Float64, strict=False).fill_null(0) >= 1
        )
    display = display.filter(problem_filter)

    keep = [c for c in ["Site", "Region", "Fuel Level", "Generator Alarm",
                          "Power Failures >8h", "R1 Unavail %",
                          "Children", "At Risk"] if c in display.columns]
    return _top_n(display.select(keep), "Fuel Level", top_n, descending=True)


# ── View 2: Thermal Risk ──────────────────────────────────────────────────────

def get_thermal_risk_sites(region: str | None = None, top_n: int = 20) -> pl.DataFrame:
    """
    Sites running hot AND with power failures.
    Hot site + power issues = hardware at accelerated risk.
    """
    temp_df  = _load("temperature_snapshot")
    avail_df = _load("availability_snapshot")
    meta     = _site_meta()

    if temp_df is None:
        return pl.DataFrame()

    temp = temp_df.select([c for c in [
        "site_id", "Avg Temperature", "Days - Extreme Temp (14 Days)",
        "Days - High Temp (14 Days)", "Temperature Ranking",
        "Number of Temperature Incidents", "Warning",
    ] if c in temp_df.columns])

    if avail_df is not None:
        avail = avail_df.select([c for c in [
            "site_id", "Num Power Failures > 8Hrs", "R1 Unavailability %",
        ] if c in avail_df.columns])
        temp = temp.join(avail, on="site_id", how="left")

    temp = temp.join(meta, on="site_id", how="left")
    temp = _filter_region(temp, region)

    # Only sites with actual thermal issues
    if "Avg Temperature" in temp.columns:
        temp = temp.filter(
            pl.col("Avg Temperature").cast(pl.Float64, strict=False).fill_null(0) >= 35
        )

    display = temp.with_columns([
        pl.col("site_name").fill_null(pl.col("site_id")).alias("Site"),
        pl.col("region").fill_null("—").alias("Region"),
    ])

    if "Avg Temperature" in temp.columns:
        display = display.with_columns(
            _status(pl.col("Avg Temperature"), (50.0, 40.0),
                    ("EXTREME", "HIGH", "NORMAL")).alias("Avg Temp °C")
        )
    if "Days - Extreme Temp (14 Days)" in temp.columns:
        display = display.with_columns(
            _status(pl.col("Days - Extreme Temp (14 Days)"), (7, 3),
                    ("EXTREME", "WARNING", "OK")).alias("Extreme Temp Days (14d)")
        )
    if "Days - High Temp (14 Days)" in temp.columns:
        display = display.with_columns(
            _status(pl.col("Days - High Temp (14 Days)"), (10, 5),
                    ("HIGH", "ELEVATED", "OK")).alias("High Temp Days (14d)")
        )
    if "Number of Temperature Incidents" in temp.columns:
        display = display.with_columns(
            _status(pl.col("Number of Temperature Incidents"), (5, 2)).alias("Temp Incidents")
        )
    if "Num Power Failures > 8Hrs" in display.columns:
        display = display.with_columns(
            _status(pl.col("Num Power Failures > 8Hrs"), (5, 2),
                    ("HIGH", "MEDIUM", "LOW")).alias("Power Failures >8h")
        )

    display = display.with_columns([
        (pl.col("n_children").cast(pl.Utf8) + pl.lit(" sites")).alias("Children"),
        (pl.col("n_risk").cast(pl.Utf8) + pl.lit(" at risk")).alias("At Risk"),
    ])

    keep = [c for c in ["Site", "Region", "Avg Temp °C", "Extreme Temp Days (14d)",
                          "High Temp Days (14d)", "Temp Incidents",
                          "Power Failures >8h", "Children", "At Risk"]
            if c in display.columns]
    sort_col = "Avg Temp °C" if "Avg Temp °C" in display.columns else "Site"
    return _top_n(display.select(keep), sort_col, top_n)


# ── View 3: Infrastructure Decay ─────────────────────────────────────────────

def get_infrastructure_sites(region: str | None = None, top_n: int = 20) -> pl.DataFrame:
    """
    Sites with rectifier failures AND generator issues — long-term decay signal.
    Both systems degraded = no safety net if grid goes down.
    """
    rect_df = _load("rectifier_snapshot")
    gen_df  = _load("generator_snapshot")
    meta    = _site_meta()

    if rect_df is None:
        return pl.DataFrame()

    rect = rect_df.select([c for c in [
        "site_id", "FailedModules %", "NumberOfModules", "FailedModules",
        "Hours Offline", "Days Rectifier No Connection", "Warning", "Online Summary",
    ] if c in rect_df.columns]).group_by("site_id").agg([
        pl.col("FailedModules %").cast(pl.Float64, strict=False).max()
          .alias("failed_pct"),
        pl.col("Hours Offline").cast(pl.Float64, strict=False).max()
          .alias("hrs_offline"),
        pl.col("Days Rectifier No Connection").cast(pl.Float64, strict=False).max()
          .alias("days_no_conn"),
        pl.col("Warning").cast(pl.Utf8).first().alias("rect_warning"),
    ])

    if gen_df is not None:
        gen = gen_df.select([c for c in [
            "site_id", "fuel_level", "alarmdesc", "RunTimeHours",
        ] if c in gen_df.columns]).group_by("site_id").agg([
            pl.col("fuel_level").cast(pl.Float64, strict=False).min().alias("fuel_raw"),
            pl.col("alarmdesc").cast(pl.Utf8).first().alias("alarm_raw"),
            pl.col("RunTimeHours").cast(pl.Float64, strict=False).max()
              .alias("gen_runtime_hrs"),
        ])
        rect = rect.join(gen, on="site_id", how="left")

    rect = rect.join(meta, on="site_id", how="left")
    rect = _filter_region(rect, region)

    # Only sites with actual rectifier problems
    rect = rect.filter(pl.col("failed_pct").fill_null(0) > 0)

    display = rect.with_columns([
        pl.col("site_name").fill_null(pl.col("site_id")).alias("Site"),
        pl.col("region").fill_null("—").alias("Region"),
        _status(pl.col("failed_pct"), (50.0, 20.0)).alias("Failed Rectifier Modules %"),
        _status(pl.col("hrs_offline"), (72.0, 24.0), ("SEVERE","WARNING","OK"))
          .alias("Hours Offline"),
        _status(pl.col("days_no_conn"), (7.0, 3.0), ("SEVERE","WARNING","OK"))
          .alias("Days No Connection"),
        (pl.col("n_children").cast(pl.Utf8) + pl.lit(" sites")).alias("Children"),
        (pl.col("n_risk").cast(pl.Utf8) + pl.lit(" at risk")).alias("At Risk"),
    ])

    if "fuel_raw" in rect.columns:
        display = display.with_columns(
            _fuel_status(pl.col("fuel_raw")).alias("Fuel Level")
        )
    if "alarm_raw" in rect.columns:
        display = display.with_columns(
            pl.col("alarm_raw").fill_null("UNKNOWN").alias("Generator Alarm")
        )

    keep = [c for c in ["Site", "Region", "Failed Rectifier Modules %",
                          "Hours Offline", "Days No Connection",
                          "Fuel Level", "Generator Alarm", "Children", "At Risk"]
            if c in display.columns]
    return _top_n(display.select(keep), "Failed Rectifier Modules %", top_n)


# ── View 4: LVD / Battery ─────────────────────────────────────────────────────

def get_lvd_battery_sites(region: str | None = None, top_n: int = 20) -> pl.DataFrame:
    """
    Sites with LVD warnings or low battery capacity.
    LVD trip = site can't ride through a power failure.
    """
    rect_df = _load("rectifier_snapshot")
    meta    = _site_meta()

    if rect_df is None:
        return pl.DataFrame()

    rect = rect_df.select([c for c in [
        "site_id", "lvd", "LVD Warning", "batterycap", "batterycharge",
        "lvdreconnect", "Hours on DC", "Hours on AC", "pld",
    ] if c in rect_df.columns]).group_by("site_id").agg([
        pl.col("lvd").cast(pl.Float64, strict=False).max().alias("lvd_flag"),
        pl.col("batterycap").cast(pl.Float64, strict=False).min().alias("batt_cap"),
        pl.col("batterycharge").cast(pl.Float64, strict=False).min().alias("batt_charge"),
        pl.col("Hours on DC").cast(pl.Float64, strict=False).max().alias("hrs_on_dc"),
        pl.col("Hours on AC").cast(pl.Float64, strict=False).min().alias("hrs_on_ac"),
    ])

    rect = rect.join(meta, on="site_id", how="left")
    rect = _filter_region(rect, region)

    # Filter: LVD triggered OR battery cap low
    rect = rect.filter(
        (pl.col("lvd_flag").fill_null(0) >= 1) |
        (pl.col("batt_cap").fill_null(100) < 80)
    )

    display = rect.with_columns([
        pl.col("site_name").fill_null(pl.col("site_id")).alias("Site"),
        pl.col("region").fill_null("—").alias("Region"),
        pl.when(pl.col("lvd_flag") >= 1).then(pl.lit("🔴 LVD ACTIVE"))
          .otherwise(pl.lit("🟢 OK")).alias("LVD Status"),
        _status(pl.col("batt_cap"), (60.0, 80.0),
                ("CRITICAL","LOW","OK")).alias("Battery Capacity %"),
        _status(pl.col("batt_charge"), (60.0, 80.0),
                ("CRITICAL","LOW","OK")).alias("Battery Charge %"),
        _status(pl.col("hrs_on_dc"), (72.0, 24.0),
                ("SEVERE","ELEVATED","NORMAL")).alias("Hours on DC"),
        (pl.col("n_children").cast(pl.Utf8) + pl.lit(" sites")).alias("Children"),
        (pl.col("n_risk").cast(pl.Utf8) + pl.lit(" at risk")).alias("At Risk"),
    ])

    keep = [c for c in ["Site", "Region", "LVD Status", "Battery Capacity %",
                          "Battery Charge %", "Hours on DC", "Children", "At Risk"]
            if c in display.columns]
    return _top_n(display.select(keep), "LVD Status", top_n, descending=True)


# ── View 5: Fuel Watch ────────────────────────────────────────────────────────

def get_fuel_watch_sites(region: str | None = None, top_n: int = 20) -> pl.DataFrame:
    """
    All generator sites sorted by lowest fuel first.
    Low fuel + long runtime = imminent outage risk.
    """
    gen_df = _load("generator_snapshot")
    meta   = _site_meta()

    if gen_df is None:
        return pl.DataFrame()

    gen = gen_df.select([c for c in [
        "site_id", "fuel_level", "FuelTank", "alarmdesc",
        "RunTimeHours", "RunHours Till Maint", "engine_rpm",
        "Days Last Communicated",
    ] if c in gen_df.columns])

    gen = gen.join(meta, on="site_id", how="left")
    gen = _filter_region(gen, region)

    display = gen.with_columns([
        pl.col("site_name").fill_null(pl.col("site_id")).alias("Site"),
        pl.col("region").fill_null("—").alias("Region"),
        pl.col("fuel_level").cast(pl.Float64, strict=False)
          .fill_null(0.0).clip(0.0, 100.0).alias("fuel_raw"),
    ])

    display = display.with_columns(
        _fuel_status(pl.col("fuel_raw")).alias("Fuel Level")
    )

    if "alarmdesc" in gen.columns:
        display = display.with_columns(
            pl.col("alarmdesc").cast(pl.Utf8).fill_null("UNKNOWN").alias("Generator Alarm")
        )
    if "RunTimeHours" in gen.columns:
        display = display.with_columns(
            _status(pl.col("RunTimeHours").cast(pl.Float64, strict=False).fill_null(0),
                    (500.0, 200.0), ("HIGH","ELEVATED","NORMAL")).alias("Runtime Hours")
        )
    if "RunHours Till Maint" in gen.columns:
        display = display.with_columns(
            _status(
                (500.0 - pl.col("RunHours Till Maint").cast(pl.Float64, strict=False).fill_null(500.0))
                .clip(0.0, 500.0),
                (400.0, 250.0), ("DUE SOON","MONITOR","OK")
            ).alias("Maintenance Urgency")
        )
    if "FuelTank" in gen.columns:
        display = display.with_columns(
            pl.col("FuelTank").cast(pl.Utf8).fill_null("—").alias("Tank Size (L)")
        )

    display = display.with_columns([
        (pl.col("n_children").cast(pl.Utf8) + pl.lit(" sites")).alias("Children"),
        (pl.col("n_risk").cast(pl.Utf8) + pl.lit(" at risk")).alias("At Risk"),
    ])

    keep = [c for c in ["Site", "Region", "Fuel Level", "Generator Alarm",
                          "Runtime Hours", "Maintenance Urgency",
                          "Tank Size (L)", "Children", "At Risk"]
            if c in display.columns]
    # Sort by fuel_raw BEFORE dropping it from select
    return display.sort("fuel_raw", descending=False).select(keep).head(top_n)


# ── View 6: Region Snapshot ───────────────────────────────────────────────────

def get_region_snapshot(region: str | None = None, top_n: int = 20) -> pl.DataFrame:
    """
    Overall worst sites for a region — priority score driven.
    Shows all 5 signal dimensions in one table.
    """
    scores = get_site_scores()
    if scores.is_empty():
        return pl.DataFrame()

    df = _filter_region(scores, region)

    display = df.with_columns([
        pl.col("site_name").fill_null(pl.col("site_id")).alias("Site"),
        pl.col("region").fill_null("—").alias("Region"),
        pl.when(pl.col("severity") == "CRITICAL").then(pl.lit("🔴 CRITICAL"))
          .when(pl.col("severity") == "HIGH").then(pl.lit("🟠 HIGH"))
          .when(pl.col("severity") == "MEDIUM").then(pl.lit("🔵 MEDIUM"))
          .otherwise(pl.lit("🟢 LOW")).alias("Severity"),
        pl.col("priority_score").round(1).alias("Priority Score"),
        pl.col("n_children").cast(pl.Int32, strict=False).fill_null(0)
          .alias("Children"),
        pl.col("n_risk").cast(pl.Int32, strict=False).fill_null(0)
          .alias("At Risk"),
    ])

    for src_col, disp_col in [
        ("availability_score",  "Availability ▲"),
        ("downtime_score",      "Downtime ▲"),
        ("temperature_score",   "Temperature ▲"),
        ("rectifier_score",     "Rectifier ▲"),
        ("generator_score",     "Generator ▲"),
    ]:
        if src_col in display.columns:
            display = display.with_columns(
                pl.col(src_col).round(1).alias(disp_col)
            )

    keep = [c for c in ["Site", "Region", "Severity", "Priority Score",
                          "Availability ▲", "Downtime ▲", "Temperature ▲",
                          "Rectifier ▲", "Generator ▲", "Children", "At Risk"]
            if c in display.columns]
    return display.select(keep).sort("Priority Score", descending=True).head(top_n)


# ── View counts (for sidebar badges) ─────────────────────────────────────────

def get_view_counts() -> dict[str, int]:
    """Return site counts per view — used for sidebar badges."""
    return {
        "power_crisis":    len(get_power_crisis_sites(top_n=9999)),
        "thermal_risk":    len(get_thermal_risk_sites(top_n=9999)),
        "infrastructure":  len(get_infrastructure_sites(top_n=9999)),
        "lvd_battery":     len(get_lvd_battery_sites(top_n=9999)),
        "fuel_watch":      len(get_fuel_watch_sites(top_n=9999)),
        "region_snapshot": len(get_region_snapshot(top_n=9999)),
    }


# ── CLI convenience ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Building scores...")
    scores = get_site_scores()
    if not scores.is_empty():
        log.info("Top 10 priority sites:")
        print(scores.head(10).select([
            "site_id", "site_name", "region", "severity",
            "priority_score", "base_score", "blast_multiplier",
            "n_children", "n_risk",
        ]))
        export_json()
