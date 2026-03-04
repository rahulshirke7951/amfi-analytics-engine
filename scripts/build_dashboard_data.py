import sqlite3
import pandas as pd
import requests
import json
import os
import gdown
import re
from datetime import datetime, timedelta

# ==========================
# 1. SETUP & CACHE CHECK
# ==========================
with open("config.json", "r") as f:
    config = json.load(f)

# Fast Cache: Check if historic.db exists (Restored by YAML cache)
if not os.path.exists("historic.db"):
    print("Downloading historic database...")
    gdown.download(config["historic_db_url"], "historic.db", quiet=True, fuzzy=True)

# mf.db changes daily, always download
release_info = requests.get(config["mf_release_api"]).json()
asset_url = next(a["browser_download_url"] for a in release_info["assets"] if a["name"] == "mf.db")
with open("mf.db", "wb") as f: 
    f.write(requests.get(asset_url).content)

# ==========================
# 2. DATA LOADING
# ==========================
conn = sqlite3.connect(":memory:")
conn.execute("ATTACH DATABASE 'mf.db' AS daily;")
conn.execute("ATTACH DATABASE 'historic.db' AS historic;")

query = """
SELECT scheme_code, nav, nav_date FROM daily.nav_history
UNION
SELECT scheme_code, nav_value AS nav, nav_date FROM historic.nav_history
"""
df = pd.read_sql_query(query, conn)

def strict_date_parse(date_str):
    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%Y/%m/%d'):
        try: return pd.to_datetime(date_str, format=fmt)
        except: continue
    return pd.to_datetime(date_str, errors='coerce')

df["nav_date"] = df["nav_date"].apply(strict_date_parse)
df = df.dropna(subset=['nav_date']).drop_duplicates(["scheme_code", "nav_date"])
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
df = df[df["nav_date"] <= today]

# ==========================
# 3. IDENTIFY STATUS & EXCLUSIONS
# ==========================
latest_nav_df = df.sort_values("nav_date").groupby("scheme_code").tail(1).copy()
freshness_threshold = today - timedelta(days=5)

# Create the master Audit Trail first
audit_trail = latest_nav_df[['scheme_code', 'nav_date', 'nav']].rename(
    columns={'nav_date': 'latest_nav_date', 'nav': 'latest_nav'}
)

# Apply the Exclusion Stamp
audit_trail['status'] = audit_trail['latest_nav_date'].apply(
    lambda x: 'Active' if x >= freshness_threshold else 'Excluded: Stale Data'
)

# ==========================
# 4. FAST VECTORIZED REINDEX (ACTIVE ONLY)
# ==========================
# Only calculate returns for Active schemes to save time
active_codes = audit_trail[audit_trail['status'] == 'Active']['scheme_code'].unique()
df_active = df[df['scheme_code'].isin(active_codes)].copy()

print(f"Processing {len(active_codes)} active schemes...")

# Only reindex the window we need (400 days)
min_date = today - timedelta(days=405)
df_active = df_active[df_active["nav_date"] >= min_date].sort_values(["scheme_code", "nav_date"])

all_dates = pd.date_range(df_active["nav_date"].min(), today, freq="D")
idx = pd.MultiIndex.from_product([active_codes, all_dates], names=["scheme_code", "nav_date"])

df_filled = (
    df_active.set_index(["scheme_code", "nav_date"])
    .reindex(idx)
    .groupby(level=0).ffill()
    .reset_index()
)

# ==========================
# 5. COMPUTE RETURNS & EXPORT
# ==========================
for d in config["return_periods_days"]:
    cutoff = today - timedelta(days=d)
    past = df_filled[df_filled["nav_date"] <= cutoff].sort_values("nav_date").groupby("scheme_code").tail(1)
    audit_trail = audit_trail.merge(past[['scheme_code', 'nav']].rename(columns={'nav': f'nav_{d}d'}), on='scheme_code', how='left')
    audit_trail[f'return_{d}d'] = ((audit_trail['latest_nav'] - audit_trail[f'nav_{d}d']) / audit_trail[f'nav_{d}d'] * 100).round(2)

# Metadata Split Logic
meta = pd.read_sql_query("SELECT DISTINCT scheme_code, scheme_name, amc_name, scheme_category FROM daily.nav_history", conn)
# (Category splitting function logic goes here...)

# FINAL SEPARATION
analytics_dashboard = audit_trail[audit_trail['status'] == 'Active'].merge(meta, on="scheme_code", how="left")

os.makedirs("output", exist_ok=True)
with pd.ExcelWriter("output/dashboard_data.xlsx", engine='xlsxwriter') as writer:
    analytics_dashboard.to_excel(writer, sheet_name="Active_Analytics", index=False)
    audit_trail.to_excel(writer, sheet_name="Full_Audit_Trail", index=False)

print("✅ Dashboard ready. Exclusions archived in Audit Trail.")
