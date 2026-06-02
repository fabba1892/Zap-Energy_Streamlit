"""
NEON HasStatic Availability – Streamlit Dashboard
==================================================
Run:  streamlit run ui/dashboard.py
"""

import json
import sys
from pathlib import Path

import polars as pl
import pandas as pd
import streamlit as st

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import queries

st.set_page_config(
    page_title="NEON Site Health",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
[data-testid="stSidebar"] { min-width: 260px; max-width: 280px; }
div[data-testid="metric-container"] > div { font-size: 0.82rem; }
/* Removed the aggressive .block-container padding to prevent hiding top elements */
</style>
""", unsafe_allow_html=True)

VIEWS = {
    "power_crisis":    {"icon": "🔴", "label": "Power Crisis",
                        "desc": "Generator running · Low fuel · Power failures combined"},
    "thermal_risk":    {"icon": "🌡️",  "label": "Thermal Risk",
                        "desc": "Sites running hot AND with power failures"},
    "infrastructure":  {"icon": "⚡",  "label": "Infrastructure Decay",
                        "desc": "Rectifier failures + generator issues — no safety net"},
    "lvd_battery":     {"icon": "🔋", "label": "LVD / Battery",
                        "desc": "LVD warnings and low battery capacity"},
    "fuel_watch":      {"icon": "⛽",  "label": "Fuel Watch",
                        "desc": "Generator sites sorted by lowest fuel first"},
    "region_snapshot": {"icon": "📊", "label": "Region Snapshot",
                        "desc": "Top 20 worst sites by overall priority score"},
}

VIEW_FNS = {
    "power_crisis":    queries.get_power_crisis_sites,
    "thermal_risk":    queries.get_thermal_risk_sites,
    "infrastructure":  queries.get_infrastructure_sites,
    "lvd_battery":     queries.get_lvd_battery_sites,
    "fuel_watch":      queries.get_fuel_watch_sites,
    "region_snapshot": queries.get_region_snapshot,
}


@st.cache_data(ttl=300)
def load_scores() -> pl.DataFrame:
    return queries.get_site_scores()


@st.cache_data(ttl=300)
def load_view_counts() -> dict:
    return queries.get_view_counts()


@st.cache_data(ttl=300)
def load_view(view_id: str, region: str) -> pl.DataFrame:
    fn = VIEW_FNS.get(view_id)
    if fn is None:
        return pl.DataFrame()
    return fn(region=None if region == "All" else region, top_n=20)


@st.cache_data(ttl=300)
def load_json_summary() -> dict:
    p = ROOT / "parquet" / "site_health.json"
    if not p.exists():
        return {}
    return json.loads(p.read_text()).get("summary", {})


@st.cache_data(ttl=300)
def get_regions(scores: pl.DataFrame) -> list[str]:
    if "region" not in scores.columns:
        return []
    return sorted(scores["region"].drop_nulls().unique().to_list())


def style_table(pdf: pd.DataFrame):
    def row_bg(row):
        text = " ".join(str(v) for v in row.values)
        if "🔴" in text:
            return ["background-color: rgba(226,75,74,0.15)"] * len(row)
        if "🟠" in text:
            return ["background-color: rgba(239,159,39,0.12)"] * len(row)
        return [""] * len(row)
    return (
        pdf.style
           .apply(row_bg, axis=1)
           .set_properties(**{"font-size": "0.83rem"})
           .hide(axis="index")
    )


def render_sidebar(scores: pl.DataFrame) -> str:
    counts = load_view_counts()

    st.sidebar.title("📡 NEON Site Health")
    st.sidebar.markdown("---")

    # ── Global KPIs in sidebar ────────────────────────────────────────────────
    s = load_json_summary()
    if s:
        st.sidebar.markdown("**Network Summary**")
        col1, col2 = st.sidebar.columns(2)
        col1.metric("Total Sites", f"{s.get('total_sites', 0):,}")
        col2.metric("Avg Priority", f"{s.get('avg_priority', 0):.1f}")
        col1.metric("🔴 Critical", s.get("critical", 0))
        col2.metric("🟠 High",     s.get("high", 0))
        col1.metric("🔵 Medium",   s.get("medium", 0))
        col2.metric("🟢 Low",      s.get("low", 0))
        st.sidebar.markdown("---")

    # ── View navigation ────────────────────────────────────────────────────────
    st.sidebar.markdown("**Problem Views**")

    if "active_view" not in st.session_state:
        st.session_state.active_view = "power_crisis"

    for vid, meta in VIEWS.items():
        cnt = counts.get(vid, 0)
        is_active = st.session_state.active_view == vid
        label = f"{meta['icon']}  {meta['label']}  ·  **{cnt}**"
        if st.sidebar.button(
            label,
            key=f"nav_{vid}",
            type="primary" if is_active else "secondary",
            use_container_width=True,
        ):
            st.session_state.active_view = vid
            st.cache_data.clear()
            st.rerun()

    # ── ETL manifest ──────────────────────────────────────────────────────────
    manifest_path = ROOT / "parquet" / "last_run.json"
    if manifest_path.exists():
        m = json.loads(manifest_path.read_text())
        st.sidebar.markdown("---")
        st.sidebar.caption(f"Last ETL: {m.get('run_timestamp', '—')[:16]}")
        site_count = m.get('site_count', '—')
        st.sidebar.caption(f"Sites in model: {site_count:,}" if isinstance(site_count, int)
                           else f"Sites in model: {site_count}")

    return st.session_state.active_view


def render_view(view_id: str, region: str) -> None:
    meta = VIEWS[view_id]
    df = load_view(view_id, region)

    # ── View headline + per-view KPIs ─────────────────────────────────────────
    st.markdown(f"## {meta['icon']}  {meta['label']}")
    st.caption(meta["desc"])

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sites shown", len(df))
    k2.metric("Region filter", region)

    if not df.is_empty() and "Fuel Level" in df.columns:
        crit = df.filter(pl.col("Fuel Level").str.starts_with("🔴")).shape[0]
        warn = df.filter(pl.col("Fuel Level").str.starts_with("🟠")).shape[0]
        k3.metric("🔴 Critical", crit)
        k4.metric("🟠 Warning",  warn)

    st.markdown("---")

    if df.is_empty():
        st.success(f"✅ No problem sites for **{meta['label']}** in **{region}**.")
        return

    # ── Table ─────────────────────────────────────────────────────────────────
    pdf = df.to_pandas()

    if view_id == "region_snapshot":
        # Use Streamlit native column config for score columns
        score_cols = [c for c in pdf.columns if "▲" in c]
        col_cfg = {
            "Priority Score": st.column_config.ProgressColumn(
                "Priority Score", min_value=0, max_value=200, format="%.1f"
            ),
            "Children": st.column_config.NumberColumn("Children 🏗️"),
            "At Risk":  st.column_config.NumberColumn("At Risk ⚠️"),
        }
        for col in score_cols:
            col_cfg[col] = st.column_config.ProgressColumn(
                col.replace(" ▲", ""), min_value=0, max_value=100, format="%.1f"
            )
        st.dataframe(pdf, use_container_width=True, hide_index=True,
                     column_config=col_cfg, height=620)
    else:
        st.dataframe(style_table(pdf), use_container_width=True, height=620)

    st.caption(f"Showing top {len(df)} sites · Sorted worst-first · "
               f"🔴 = Critical action needed · 🟠 = Monitor · 🟢 = OK")


def main() -> None:
    scores = load_scores()

    if scores.is_empty():
        st.error(
            "No data loaded. Run the ETL pipeline first:\n\n"
            "```\npython scripts/build_analytical_db.py\n```"
        )
        st.stop()

    active_view = render_sidebar(scores)
    regions = get_regions(scores)

    # ── Region Pills (Stateful & Persistent) ──────────────────────────────────
    region_options = ["All"] + regions

    # 1. Initialize the session state for the region picker if it doesn't exist
    if "region_picker" not in st.session_state:
        st.session_state.region_picker = "All"

    # 2. Callback: Catch the "unclick" (None) and force it back to "All"
    def enforce_region_selection():
        if st.session_state.region_picker is None:
            st.session_state.region_picker = "All"

    # 3. Render the pills, tied strictly to the session state key and callback
    st.pills(
        "🌍 Filter by Region:", 
        options=region_options, 
        key="region_picker",
        on_change=enforce_region_selection
    )

    # 4. Render the active view using the persistent session state value
    render_view(active_view, st.session_state.region_picker)


if __name__ == "__main__":
    main()
