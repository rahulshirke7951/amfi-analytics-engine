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

ref_code = str(config.get("audit_scheme_code", 140088))
log.info(f"--- 🔄 SAFE CONSOLIDATION START: Scheme {ref_code} ---")

# DOWNLOAD ONLY IF MISSING (Preserves your files)
if not os.path.exists("historic.db"):
    log.info("historic.db missing, downloading...")
    gdown.download(config["historic_db_url"], "historic.db", quiet=True, fuzzy=True)

if not os.path.exists("mf.db"):
    log.info("mf.db missing, downloading daily snapshot...")
    release_info = requests.get(config["mf_release_api"]).json()
    asset_url = next(a["browser_download_url"] for a in release_info["assets"] if a["name"] == "mf.db")
    with open("mf.db", "wb") as f:
        f.write(requests.get(asset_url).content)

# ==========================
# 2. BRUTE-FORCE CONSOLIDATION
# ==========================
with sqlite3.connect(":memory:") as conn:
    conn.execute("ATTACH DATABASE 'mf.db' AS daily;")
    conn.execute("ATTACH DATABASE 'historic.db' AS historic;")
    
    # Standard UNION logic
    query = """
        SELECT CAST(scheme_code AS TEXT) as scheme_code, nav, nav_date, 'daily' AS source FROM daily.nav_history
        UNION ALL
        SELECT CAST(scheme_code AS TEXT) as scheme_code, nav_value AS nav, nav_date, 'historic' AS source FROM historic.nav_history
    """
    df_raw = pd.read_sql_query(query, conn)

# 1. Standardize Dates immediately (Crucial for March 4/5 visibility)
df_raw["nav_date"] = pd.to_datetime(df_raw["nav_date"], errors='coerce')
df_raw = df_raw.dropna(subset=["nav_date"])

# 2. Priority Sort: ensure 'daily' sits on top of 'historic' for the same date
df_raw = df_raw.sort_values(by=["nav_date", "source"], ascending=[True, True])

# 3. Consolidate: This keeps the first occurrence (daily) and drops the rest
df_consolidated = df_raw.drop_duplicates(subset=["scheme_code", "nav_date"], keep="first")

# 4. Filter for Audit Scheme
df_scheme = df_consolidated[df_consolidated["scheme_code"] == ref_code].copy()
df_scheme = df_scheme.set_index("nav_date").sort_index()

if df_scheme.empty:
    log.error(f"❌ Scheme {ref_code} still not found. Please verify the code in AMFI.")
    exit(1)

# ==========================
# 3. ANCHOR & AUDIT
# ==========================
latest_available = df_scheme.index.max()
# Anchor is 1 day before the max available date in the dataset
today = (latest_available - timedelta(days=1)).replace(hour=0, minute=0, second=0)

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

# ==========================
# 4. EXPORT
# ==========================
os.makedirs("output", exist_ok=True)
with pd.ExcelWriter("output/audit_debug_report.xlsx", engine='openpyxl') as writer:
    df_scheme.reset_index().sort_values("nav_date", ascending=False).to_excel(writer, sheet_name="Full_NAV_History", index=False)
    pd.DataFrame(audit_results).to_excel(writer, sheet_name="Returns_Audit", index=False)

log.info(f"✅ Safe Consolidation Complete. Latest date: {latest_available.date()}")
