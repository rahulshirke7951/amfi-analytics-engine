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
    
    # 🛠️ "Type-Safe" Fetching: We convert scheme_code to string during search to avoid type mismatches
    query = """
        SELECT CAST(scheme_code AS TEXT) as scheme_code, nav, nav_date, 'daily' AS source FROM daily.nav_history
        UNION ALL
        SELECT CAST(scheme_code AS TEXT) as scheme_code, nav_value AS nav, nav_date, 'historic' AS source FROM historic.nav_history
    """
    df_all = pd.read_sql_query(query, conn)

# Deduplication
df_all["nav_date"] = parse_dates_vectorized(df_all["nav_date"])
df_all = df_all.dropna(subset=["nav_date"])
df_dedup = df_all.sort_values(["scheme_code", "nav_date", "source"]).drop_duplicates(["scheme_code", "nav_date"], keep="first")

# Derive Anchor Date
latest_nav_date = df_dedup["nav_date"].max()
today = (latest_nav_date - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

# Filter for the Audit Scheme (Handling both Int and Str)
df_scheme = df_dedup[df_dedup["scheme_code"] == str(ref_code)].copy()

# 🛠️ THE DISCOVERY LOGIC
if df_scheme.empty:
    log.error(f"❌ No data found for Scheme Code {ref_code}.")
    
    # Check if the code is actually in the DB but under a different type
    available = df_dedup["scheme_code"].unique()
    log.info(f"💡 Total unique codes in DB: {len(available)}")
    
    # Suggest similar codes (first 3 digits match)
    suggestions = [c for c in available if str(c)[:3] == str(ref_code)[:3]]
    if suggestions:
        log.info(f"🧐 Did you mean one of these codes? {suggestions[:5]}")
    
    # Export what we have to help you debug
    os.makedirs("output", exist_ok=True)
    pd.DataFrame({"Available_Codes": available}).to_excel("output/audit_debug_report.xlsx")
    log.info("Saved list of available codes to output/audit_debug_report.xlsx for your reference.")
    exit(0)

# ==========================
# 3. RETURN CALCULATION AUDIT (As before)
# ==========================
df_scheme = df_scheme.set_index("nav_date").sort_index()
current_nav_row = df_scheme[:today].tail(1)
current_nav = current_nav_row['nav'].values[0] if not current_nav_row.empty else 0

audit_results = []
for d in config.get("return_periods_days", [30, 365]):
    target_date = today - timedelta(days=d)
    past_data = df_scheme[:target_date].tail(1)
    past_nav = past_data['nav'].values[0] if not past_data.empty else None
    ret = round(((current_nav - past_nav) / past_nav) * 100, 2) if past_nav else None
    
    audit_results.append({
        "Period_Days": d,
        "Target_Date": target_date.date(),
        "Matched_Date": past_data.index[0].date() if not past_data.empty else "MISSING",
        "Start_NAV": past_nav,
        "End_NAV": current_nav,
        "Return_Pct": ret
    })

# Export
os.makedirs("output", exist_ok=True)
with pd.ExcelWriter("output/audit_debug_report.xlsx", engine='openpyxl') as writer:
    df_scheme.reset_index().sort_values("nav_date", ascending=False).to_excel(writer, sheet_name="Full_NAV_History", index=False)
    pd.DataFrame(audit_results).to_excel(writer, sheet_name="Returns_Audit", index=False)

log.info("✅ Audit Complete.")
