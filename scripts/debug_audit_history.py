import sqlite3
import pandas as pd
import requests
import json
import os
import gdown
from datetime import datetime

# Load Configuration
with open("config.json", "r") as f:
    config = json.load(f)

ref_code = config.get("audit_scheme_code", 149758)
historic_url = config.get("historic_db_url")
daily_api = config.get("mf_release_api")

print(f"--- 🛠️ UPDATED DEBUG AUDIT START: Scheme {ref_code} ---")

# Download databases
if not os.path.exists("historic.db"):
    gdown.download(historic_url, "historic.db", quiet=True, fuzzy=True)

release_info = requests.get(daily_api).json()
asset_url = next(a["browser_download_url"] for a in release_info["assets"] if a["name"] == "mf.db")
with open("mf.db", "wb") as f: 
    f.write(requests.get(asset_url).content)

conn = sqlite3.connect(":memory:")
conn.execute("ATTACH DATABASE 'mf.db' AS daily;")
conn.execute("ATTACH DATABASE 'historic.db' AS historic;")

# 1. Fetch RAW strings from both sources
df_daily = pd.read_sql_query(f"SELECT nav_date, nav, 'DAILY_PIPELINE' as source FROM daily.nav_history WHERE scheme_code = {ref_code}", conn)
df_hist = pd.read_sql_query(f"SELECT nav_date, nav_value as nav, 'HISTORIC_DB' as source FROM historic.nav_history WHERE scheme_code = {ref_code}", conn)

df_combined = pd.concat([df_daily, df_hist])

# 🛠️ APPLY THE PRODUCTION-GRADE STRICT PARSING
def strict_date_parse(date_str):
    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%Y/%m/%d'):
        try:
            return pd.to_datetime(date_str, format=fmt)
        except (ValueError, TypeError):
            continue
    return pd.to_datetime(date_str, errors='coerce')

df_combined['python_parsed_date'] = df_combined['nav_date'].apply(strict_date_parse)

# Sort by the parsed date to see the timeline clearly
df_combined = df_combined.sort_values('python_parsed_date', ascending=False)

# Save specialized debug output
os.makedirs("output", exist_ok=True)
output_path = "output/audit_debug_report.xlsx"

with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    df_combined.to_excel(writer, sheet_name="Timeline_Comparison", index=False)
    
    # Summary of the two databases
    summary = pd.DataFrame([
        {"Source": "Daily DB", "Start": df_daily['nav_date'].min(), "End": df_daily['nav_date'].max(), "Count": len(df_daily)},
        {"Source": "Historic DB", "Start": df_hist['nav_date'].min(), "End": df_hist['nav_date'].max(), "Count": len(df_hist)}
    ])
    summary.to_excel(writer, sheet_name="Data_Source_Summary", index=False)

print(f"✅ Debug Report with Strict Parsing Generated: {output_path}")
