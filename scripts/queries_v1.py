import polars as pl
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
PARQUET = BASE_DIR / "parquet"

DIM = PARQUET / "dimensions"
FACT = PARQUET / "facts"

# --------------------------------------------------
# Load helpers (lazy by default)
# --------------------------------------------------
def availability():
    return pl.scan_parquet(FACT / "availability_snapshot.parquet")

def temperature():
    return pl.scan_parquet(FACT / "temperature_snapshot.parquet")

def site_dim():
    return pl.scan_parquet(DIM / "site.parquet")

def site_to_atoll():
    return pl.scan_parquet(DIM / "site_to_atoll.parquet")

def ci_to_site():
    return pl.scan_parquet(DIM / "ci_to_site.parquet")

# --------------------------------------------------
# CORE QUERY 1:
# Static generator but low availability
# --------------------------------------------------
def static_generator_low_availability(
    availability_threshold: float = 97.0
):
    """
    Sites with static generators but poor availability.
    This is your primary diagnostic use case.
    """

    avail = availability()
    atoll = site_to_atoll()
    site = site_dim()

    df = (
        avail
        .join(site, on="SiteId", how="left")
        .join(atoll, on="SiteId", how="left")
        .filter(pl.col("availability_pct") < availability_threshold)
        .select([
            "SiteId",
            "SiteName",
            "Atoll",
            "availability_pct",
            "snapshot_ts"
        ])
        .sort("availability_pct")
    )

    return df.collect()

# --------------------------------------------------
# CORE QUERY 2:
# Availability distribution by Atoll
# --------------------------------------------------
def availability_by_atoll():
    avail = availability()
    atoll = site_to_atoll()

    return (
        avail
        .join(atoll, on="SiteId", how="left")
        .group_by("Atoll")
        .agg([
            pl.mean("availability_pct").alias("avg_availability"),
            pl.count().alias("site_count")
        ])
        .sort("avg_availability")
        .collect()
    )

# --------------------------------------------------
# CORE QUERY 3:
# Unmapped / problematic records (data quality)
# --------------------------------------------------
def unmapped_sites():
    site = site_dim()
    atoll = site_to_atoll()

    return (
        site
        .join(atoll, on="SiteId", how="left")
        .filter(pl.col("Atoll").is_null())
        .collect()
    )

# --------------------------------------------------
# CORE QUERY 4:
# Trend for a single site (time-series)
# --------------------------------------------------
def availability_trend(site_id: str):
    return (
        availability()
        .filter(pl.col("SiteId") == site_id)
        .select([
            "snapshot_ts",
            "availability_pct"
        ])
        .sort("snapshot_ts")
        .collect()
    )
