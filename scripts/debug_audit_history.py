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
log.info(f"--- 🔍 TRANSPARENT MIRROR DEBUG START: Scheme {ref_code} ---")

# Standard Downloads
if not os.path.exists("historic.db"):
    gdown.download(config["historic_db_url"], "historic.db", quiet=True, fuzzy=True)

release_info = requests.get(config["mf_release_api"]).json()
asset_url = next(a["browser_download_url"] for a in release_info["assets"] if a["name"] == "mf.db")
with open("mf.db", "wb") as f:
    f.write(requests.get(asset_url).content)

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
        SELECT CAST(scheme_code AS TEXT) as scheme_code, nav, nav_date, 'daily' AS source FROM daily.nav_history
        UNION ALL
        SELECT CAST(scheme_code AS TEXT) as scheme_code, nav_value AS nav, nav_date, 'historic' AS source FROM historic.nav_history
    """
    df_all = pd.read_sql_query(query, conn)

df_all["nav_date"] = parse_dates_vectorized(df_all["nav_date"])
df_all = df_all.dropna(subset=["nav_date"])
df_dedup = df_all.sort_values(["scheme_code", "nav_date", "source"]).drop_duplicates(["scheme_code", "nav_date"], keep="first")

latest_nav_date = df_dedup["nav_date"].max()
today = (latest_nav_date - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

df_scheme = df_dedup[df_dedup["scheme_code"] == str(ref_code)].copy()

if df_scheme.empty:
    log.error(f"❌ No data found for Scheme Code {ref_code}.")
    exit(0)

# ==========================
# 3. RETURN CALCULATION AUDIT (UNTOUCHED)
# ==========================
df_scheme_indexed = df_scheme.set_index("nav_date").sort_index()
current_nav_row = df_scheme_indexed[:today].tail(1)
current_nav = current_nav_row['nav'].values[0] if not current_nav_row.empty else 0

audit_results = []
for d in return_periods:
    target_date = today - timedelta(days=d)
    past_data = df_scheme_indexed[:target_date].tail(1)
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
# 4. CONTINUOUS HISTORY WITH SOURCE TAGS
# ==========================
full_range = pd.date_range(start=df_scheme_indexed.index.min(), end=latest_nav_date, freq='D')

# Reindex but handle the source column carefully
df_history_final = df_scheme_indexed.reindex(full_range)

# Identify rows that were just created by reindex (where nav is NaN)
df_history_final.loc[df_history_final['nav'].isna(), 'source'] = 'ffill'

# Now forward fill the NAV and scheme_code values
df_history_final[['nav', 'scheme_code']] = df_history_final[['nav', 'scheme_code']].ffill()

df_history_final = (
    df_history_final.reset_index()
    .rename(columns={'index': 'nav_date'})
    .sort_values("nav_date", ascending=False)
)

# ==========================
# 5. EXPORT
# ==========================
os.makedirs("output", exist_ok=True)
with pd.ExcelWriter("output/audit_debug_report.xlsx", engine='openpyxl') as writer:
    df_history_final.to_excel(writer, sheet_name="Full_NAV_History", index=False)
    pd.DataFrame(audit_results).to_excel(writer, sheet_name="Returns_Audit", index=False)
    
    pd.DataFrame([{
        "Ref_Scheme": ref_code, 
        "Anchor_Date": today.date(), 
        "Source_Legend": "daily/historic = Real Data | ffill = Data gap filled from previous day"
    }]).to_excel(writer, sheet_name="Config_Context", index=False)

log.info("✅ Audit Complete. Transparent History generated.")
