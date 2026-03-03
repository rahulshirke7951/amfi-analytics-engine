import sqlite3
import pandas as pd
import requests
import json
import os
import gdown
from datetime import datetime

# Load Configuration (Shared with production)
with open("config.json", "r") as f:
    config = json.load(f)

# Pull settings from JSON
ref_code = config.get("audit_scheme_code", 149758)
historic_url = config.get("historic_db_url")
daily_api = config.get("mf_release_api")

print(f"--- 🛠️ DEBUG AUDIT START: Scheme {ref_code} ---")

# Download databases if missing
if not os.path.exists("historic.db"):
    gdown.download(historic_url, "historic.db", quiet=False, fuzzy=True)

release_info = requests.get(daily_api).json()
asset_url = next(a["browser_download_url"] for a in release_info["assets"] if a["name"] == "mf.db")
with open("mf.db", "wb") as f: 
    f.write(requests.get(asset_url).content)

# Connect to both databases
conn = sqlite3.connect(":memory:")
conn.execute("ATTACH DATABASE 'mf.db' AS daily;")
conn.execute("ATTACH DATABASE 'historic.db' AS historic;")

# 1. Fetch RAW data from Daily Pipeline
df_daily = pd.read_sql_query(f"SELECT nav_date, nav, 'DAILY_PIPELINE' as source FROM daily.nav_history WHERE scheme_code = {ref_code}", conn)

# 2. Fetch RAW data from Historic DB
df_hist = pd.read_sql_query(f"SELECT nav_date, nav_value as nav, 'HISTORIC_DB' as source FROM historic.nav_history WHERE scheme_code = {ref_code}", conn)

# 3. Combine for Audit
df_combined = pd.concat([df_daily, df_hist])

# 4. Debug Date Interpretation
# We show the Raw String vs what Python thinks the date is
df_combined['python_parsed_date'] = pd.to_datetime(df_combined['nav_date'], format='mixed', dayfirst=True, errors='coerce')

# Sort by newest first
df_combined = df_combined.sort_values('python_parsed_date', ascending=False)

# Save to specialized output
os.makedirs("output", exist_ok=True)
output_path = "output/audit_debug_report.xlsx"

with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    df_combined.to_excel(writer, sheet_name="Timeline_Comparison", index=False)
    
    # Summary Sheet
    summary = pd.DataFrame([
        {"Source": "Daily DB", "Start": df_daily['nav_date'].min(), "End": df_daily['nav_date'].max(), "Rows": len(df_daily)},
        {"Source": "Historic DB", "Start": df_hist['nav_date'].min(), "End": df_hist['nav_date'].max(), "Rows": len(df_hist)}
    ])
    summary.to_excel(writer, sheet_name="Source_Summary", index=False)

print(f"✅ Debug Report Generated: {output_path}")
