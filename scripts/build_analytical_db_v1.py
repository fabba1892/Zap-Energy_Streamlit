from pathlib import Path
from datetime import datetime
import polars as pl

# ------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
PARQUET_DIR = BASE_DIR / "parquet"

DIM_DIR = PARQUET_DIR / "dimensions"
FACT_DIR = PARQUET_DIR / "facts"

DIM_DIR.mkdir(parents=True, exist_ok=True)
FACT_DIR.mkdir(parents=True, exist_ok=True)

SNAPSHOT_TS = datetime.utcnow()

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def read_excel(file_name, sheet=0):
    return pl.read_excel(DATA_DIR / file_name, sheet_id=sheet)

def write_parquet(df, path):
    df.write_parquet(path, compression="zstd")

# ------------------------------------------------------------------
# 1️⃣ Load raw reports
# ------------------------------------------------------------------
paas = read_excel("PAAS_Availability_Weekly_Analysis_detail_20260406.xlsx")
temperature = read_excel("RAN_Site_Temperature_Analysis_20260409.xlsx")
generator = read_excel("GeneratorSummary_2026_03_27.xlsx")
power_failure = read_excel("Tx Hub Site Power Failure_Open_Daily 2026-04-08-02-30-10.xlsx")

# ------------------------------------------------------------------
# 2️⃣ DIMENSION: Site (canonical SiteId reference)
# ------------------------------------------------------------------
site_dim = (
    pl.concat([
        paas.select(["SiteId", "SiteName"]),
        temperature.select(["SiteId", "SiteName"])
    ])
    .unique(subset=["SiteId"])
    .with_columns([
        pl.lit(SNAPSHOT_TS).alias("load_ts"),
        pl.lit("site_dimension").alias("source")
    ])
)

write_parquet(site_dim, DIM_DIR / "site.parquet")

# ------------------------------------------------------------------
# 3️⃣ DIMENSION: Site → Atoll mapping
# ------------------------------------------------------------------
site_to_atoll = (
    paas.select(["SiteId"])
    .unique()
    .join(
        generator.select(["Atoll"]).unique(),
        how="cross"  # refined later if logic improves
    )
    .with_columns(pl.lit(SNAPSHOT_TS).alias("load_ts"))
)

write_parquet(site_to_atoll, DIM_DIR / "site_to_atoll.parquet")

# ------------------------------------------------------------------
# 4️⃣ DIMENSION: CI Name → SiteId bridge (SPECIAL CASE)
# ------------------------------------------------------------------
ci_to_site = (
    power_failure
    .select(pl.col("CI Name").alias("ci_name"))
    .unique()
    .join(
        paas.select(
            pl.col("SiteName").alias("site_name"),
            pl.col("SiteId").alias("site_id")
        ),
        left_on="ci_name",
        right_on="site_name",
        how="left"
    )
    .with_columns([
        pl.lit("paas_match").alias("mapping_type"),
        pl.lit(SNAPSHOT_TS).alias("load_ts")
    ])
)

write_parquet(ci_to_site, DIM_DIR / "ci_to_site.parquet")

# ------------------------------------------------------------------
# 5️⃣ FACT: Availability snapshot
# ------------------------------------------------------------------
availability_fact = (
    paas
    .select([
        "SiteId",
        pl.col("Uptime_Percentage").alias("availability_pct")
    ])
    .with_columns([
        pl.lit(SNAPSHOT_TS).alias("snapshot_ts"),
        pl.lit("PAAS").alias("source")
    ])
)

write_parquet(
    availability_fact,
    FACT_DIR / "availability_snapshot.parquet"
)

# ------------------------------------------------------------------
# 6️⃣ FACT: Temperature snapshot
# ------------------------------------------------------------------
temperature_fact = (
    temperature
    .select([
        "SiteId",
        pl.col("Temperature").alias("temperature_c")
    ])
    .with_columns([
        pl.lit(SNAPSHOT_TS).alias("snapshot_ts"),
        pl.lit("TEMP").alias("source")
    ])
)

write_parquet(
    temperature_fact,
    FACT_DIR / "temperature_snapshot.parquet"
)

print("✅ Analytical Parquet model built successfully")