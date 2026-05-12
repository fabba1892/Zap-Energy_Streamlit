import streamlit as st
import duckdb
from pathlib import Path

CSV_DB_FOLDER = Path("csv_db")

st.title("Neon HasStatic Availability Dashboard")

# Connect to DuckDB
conn = duckdb.connect(CSV_DB_FOLDER / "analytical.duckdb", read_only=True)

# Sidebar filters
atoll = st.sidebar.selectbox("Select Atoll", 
    conn.execute("SELECT DISTINCT Atoll FROM generator_summary").pl()['Atoll']
)

# Query availability by Atoll
data = conn.execute(f"""
    SELECT SiteId, SiteName, Uptime_Percentage 
    FROM paas 
    WHERE Atoll = '{atoll}'
    ORDER BY Uptime_Percentage DESC
""").pl()

# Display in Streamlit
st.dataframe(data)

# Simple chart
st.bar_chart(data.to_pandas().set_index('SiteId')['Uptime_Percentage'])