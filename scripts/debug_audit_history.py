import sqlite3
import pandas as pd
import requests
import json
import os
import gdown
import logging
from datetime import datetime, timedelta

# ==========================
# LOGGING SETUP
# ==========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ==========================
# 1. LOAD CONFIG & ARTIFACTS
# ==========================
with open("config.json", "r") as f:
    config = json.load(f)

ref_code = config.get("audit_scheme_code", 149758)
return_periods = config.get("return_periods_days", [30, 90, 365])
log.info(f"--- 🔍 ROBUST MIRROR DEBUG: Scheme {ref_code} ---")

# Standard Downloads
if not os.path.exists("historic.db"):
    gdown.download(config["historic_db_url"], "historic.db", quiet=True, fuzzy=True)

release_info = requests.get(config["mf_release_api"]).json()
asset_url = next(a["browser_download_url"] for a in release_info["assets"] if a["name"] == "mf.db")
with open("mf.db", "wb") as f:
    f.write(requests.get(asset_url).content)

# ==========================
# 2. REPLICATE PRODUCTION LOGIC
# ==========================

def parse_dates_vectorized(series: pd.Series) -> pd.Series:
    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%Y/%m/%d'):
        parsed = pd.to_datetime(series, format=fmt, errors='coerce')
        if parsed.notna().mean() > 0.95: return parsed
    return pd.to_datetime(series, errors='coerce')

with sqlite3.connect(":memory:") as conn:
    conn.execute("ATTACH DATABASE 'mf.db' AS daily;")
    conn.execute("ATTACH DATABASE 'historic.db' AS historic;")
    query = """
        SELECT scheme_code, nav, nav_date, 'daily' AS source FROM daily.nav_history
        UNION ALL
        SELECT scheme_code, nav_value AS nav, nav_date, 'historic' AS source FROM historic.nav_history
    """
    df_all = pd.read_sql_query(query, conn)

# Deduplication & Anchor Date
df_all["nav_date"] = parse_dates_vectorized(df_all["nav_date"])
df_all = df_all.dropna(subset=["nav_date"])
df_dedup = df_all.sort_values(["scheme_code", "nav_date", "source"]).drop_duplicates(["scheme_code", "nav_date"], keep="first")

latest_nav_date = df_dedup["nav_date"].max()
today = (latest_nav_date - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

# Filter for Audit Scheme
df_scheme = df_dedup[df_dedup["scheme_code"] == ref_code].copy()
df_scheme = df_scheme.sort_values("nav_date")

# Replicate Reindexing/FFill for this scheme
all_dates = pd.date_range(df_scheme["nav_date"].min(), today, freq="D")
df_filled = df_scheme.set_index("nav_date").reindex(all_dates).ffill().reset_index().rename(columns={"index": "nav_date"})

# ==========================
# 3. CALCULATE RETURNS FOR AUDIT
# ==========================
latest_nav = df_filled.iloc[-1]["nav"]
latest_date = df_filled.iloc[-1]["nav_date"]

calc_audit = []
for d in return_periods:
    target_dt = today - timedelta(days=d)
    # Finding the NAV on or before target date
    past_data = df_filled[df_filled["nav_date"] <= target_dt].iloc[-1:]
    
    if not past_data.empty:
        past_nav = past_data["nav"].values[0]
        past_date = past_data["nav_date"].values[0]
        ret = round(((latest_nav - past_nav) / past_nav) * 100, 2)
        calc_audit.append({
            "Period_Days": d,
            "Target_Date": target_dt.date(),
            "Actual_Past_Date": past_date.date(),
            "Past_NAV": past_nav,
            "Latest_NAV": latest_nav,
            "Return_Percent": ret
        })

# ==========================
# 4. EXPORT
# ==========================
os.makedirs("output", exist_ok=True)
output_path = "output/audit_debug_report.xlsx"

with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    pd.DataFrame(calc_audit).to_excel(writer, sheet_name="Return_Calculations", index=False)
    df_scheme.sort_values("nav_date", ascending=False).to_excel(writer, sheet_name="Raw_Timeline", index=False)
    
    logic_summary = pd.DataFrame([{
        "Audit_Scheme": ref_code,
        "Anchor_Date": today.date(),
        "Latest_Data_Point": latest_date.date()
    }])
    logic_summary.to_excel(writer, sheet_name="Logic_Summary", index=False)

log.info(f"✅ Debug Report with Returns Generated: {output_path}")
