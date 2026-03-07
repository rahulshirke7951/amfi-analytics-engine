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

ref_code = config.get("audit_scheme_code", 140088)
return_periods = config.get("return_periods_days", [30, 365, 1095])
log.info(f"--- 🔍 ROBUST MIRROR DEBUG START: Scheme {ref_code} ---")

# Standard Downloads
if not os.path.exists("historic.db"):
    gdown.download(config["historic_db_url"], "historic.db", quiet=True, fuzzy=True)

try:
    release_info = requests.get(config["mf_release_api"]).json()
    asset_url = next(a["browser_download_url"] for a in release_info["assets"] if a["name"] == "mf.db")
    with open("mf.db", "wb") as f:
        f.write(requests.get(asset_url).content)
except Exception as e:
    log.error(f"Download failed: {e}")
    raise

# ==========================
# 2. DATA PIPELINE REPLICATION
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

# Deduplication & Anchor Logic (Mirror of Dashboard Script)
df_all["nav_date"] = parse_dates_vectorized(df_all["nav_date"])
df_all = df_all.dropna(subset=["nav_date"])

if df_all.empty:
    raise RuntimeError("Global dataset is empty. Check DB files.")

df_dedup = df_all.sort_values(["scheme_code", "nav_date", "source"]).drop_duplicates(["scheme_code", "nav_date"], keep="first")

latest_nav_date = df_dedup["nav_date"].max()
today = (latest_nav_date - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

# Filter for the Audit Scheme
df_scheme = df_dedup[df_dedup["scheme_code"] == ref_code].copy()

# --- THE FIX: Handle empty scheme data or NaT ---
if df_scheme.empty:
    log.error(f"❌ No data found for Scheme Code {ref_code}. Check if the code is correct.")
    # Create empty excel to satisfy workflow
    os.makedirs("output", exist_ok=True)
    pd.DataFrame([{"Error": f"Scheme {ref_code} not found"}]).to_excel("output/audit_debug_report.xlsx")
    exit(0) 

# ==========================
# 3. RETURN CALCULATION AUDIT
# ==========================
df_scheme = df_scheme.set_index("nav_date").sort_index()

# Get the Current NAV at anchor
current_nav_row = df_scheme[:today].tail(1)
if current_nav_row.empty:
    log.warning(f"No NAV found for {ref_code} on or before {today.date()}")
    current_nav = 0
    actual_current_date = None
else:
    current_nav = current_nav_row['nav'].values[0]
    actual_current_date = current_nav_row.index[0]

audit_results = []
for d in return_periods:
    target_date = today - timedelta(days=d)
    past_data = df_scheme[:target_date].tail(1)
    
    if not past_data.empty:
        past_nav = past_data['nav'].values[0]
        past_date = past_data.index[0]
        ret = round(((current_nav - past_nav) / past_nav) * 100, 2)
    else:
        past_nav, past_date, ret = None, None, None
        
    audit_results.append({
        "Period_Days": d,
        "Target_Date": target_date.date(),
        "Matched_Date": past_date.date() if past_date else "MISSING",
        "Start_NAV": past_nav,
        "End_NAV": current_nav,
        "Return_Pct": ret
    })

# ==========================
# 4. EXPORT
# ==========================
os.makedirs("output", exist_ok=True)
with pd.ExcelWriter("output/audit_debug_report.xlsx", engine='openpyxl') as writer:
    df_scheme.reset_index().sort_values("nav_date", ascending=False).to_excel(writer, sheet_name="Full_NAV_History", index=False)
    pd.DataFrame(audit_results).to_excel(writer, sheet_name="Returns_Audit", index=False)
    
    # Global context tab
    pd.DataFrame([{
        "Ref_Scheme": ref_code, 
        "Anchor_Date": today.date(), 
        "Global_Latest": latest_nav_date.date(),
        "Scheme_Latest": df_scheme.index.max().date()
    }]).to_excel(writer, sheet_name="Config_Context", index=False)

log.info(f"✅ Audit Complete. Report: output/audit_debug_report.xlsx")
