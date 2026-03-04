import sqlite3
import pandas as pd
import requests
import json
import os
import gdown
import re
from datetime import datetime, timedelta

# ==========================
# 1. LOAD CONFIG & DOWNLOADS
# ==========================
with open("config.json", "r") as f:
    config = json.load(f)

# Use cached file if available (Historic DB)
if not os.path.exists("historic.db"):
    print("Cache miss: Downloading historic database from Google Drive...")
    gdown.download(config["historic_db_url"], "historic.db", quiet=True, fuzzy=True)
else:
    print("Cache hit: Using cached historic.db from GitHub Storage")

print("Fetching daily mf.db from GitHub API...")
try:
    response = requests.get(config["mf_release_api"])
    release_info = response.json()
    
    # Safety Check for API response
    if "assets" in release_info:
        asset_url = next(a["browser_download_url"] for a in release_info["assets"] if a["name"] == "mf.db")
        with open("mf.db", "wb") as f: 
            f.write(requests.get(asset_url).content)
        print("✅ mf.db downloaded successfully.")
    else:
        print(f"❌ API Error: 'assets' key not found. Response received: {release_info}")
        exit(1)
except Exception as e:
    print(f"❌ Failed to download mf.db: {e}")
    exit(1)

# ==========================
# 2. DATA LOADING & CLEANING
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
# 3. IDENTIFY STATUS (STALE VS ACTIVE)
# ==========================
latest_nav_df = df.sort_values("nav_date").groupby("scheme_code").tail(1).copy()
freshness_threshold = today - timedelta(days=5)

audit_trail = latest_nav_df[['scheme_code', 'nav_date', 'nav']].rename(
    columns={'nav_date': 'latest_nav_date', 'nav': 'latest_nav'}
)

audit_trail['status'] = audit_trail['latest_nav_date'].apply(
    lambda x: 'Active' if x >= freshness_threshold else 'Excluded: Stale Data'
)

# ==========================
# 4. FAST VECTORIZED REINDEX (ACTIVE ONLY)
# ==========================
active_codes = audit_trail[audit_trail['status'] == 'Active']['scheme_code'].unique()
df_active = df[df['scheme_code'].isin(active_codes)].copy()

print(f"Processing {len(active_codes)} active schemes for returns...")

# Only reindex the window we need (last 405 days) to save time
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
# 5. COMPUTE RETURNS
# ==========================
for d in config["return_periods_days"]:
    cutoff = today - timedelta(days=d)
    past = df_filled[df_filled["nav_date"] <= cutoff].sort_values("nav_date").groupby("scheme_code").tail(1)
    audit_trail = audit_trail.merge(past[['scheme_code', 'nav']].rename(columns={'nav': f'nav_{d}d'}), on='scheme_code', how='left')
    
    mask = (audit_trail['status'] == 'Active') & (audit_trail[f'nav_{d}d'].notna())
    audit_trail.loc[mask, f'return_{d}d'] = ((audit_trail['latest_nav'] - audit_trail[f'nav_{d}d']) / audit_trail[f'nav_{d}d'] * 100).round(2)

# Metadata processing
meta = pd.read_sql_query("SELECT DISTINCT scheme_code, scheme_name, amc_name, scheme_category FROM daily.nav_history", conn)

def split_category(cat_str):
    if not cat_str or '(' not in cat_str: return pd.Series([cat_str or 'NA', 'NA', 'NA'])
    main = cat_str.split('(')[0].strip()
    sub_part = re.search(r'\((.*?)\)', cat_str)
    if sub_part:
        sub_content = sub_part.group(1).split(' - ')
        return pd.Series([main, sub_content[0].strip() if len(sub_content) > 0 else 'NA', sub_content[1].strip() if len(sub_content) > 1 else 'NA'])
    return pd.Series([main, 'NA', 'NA'])

meta[['cat_level_1', 'cat_level_2', 'cat_level_3']] = meta['scheme_category'].apply(split_category)
meta['plan_type'] = meta['scheme_name'].apply(lambda x: 'Direct' if 'Direct' in str(x) else 'Regular')

# ==========================
# 6. FINAL EXPORT
# ==========================
analytics_dashboard = audit_trail[audit_trail['status'] == 'Active'].merge(meta, on="scheme_code", how="left")

os.makedirs("output", exist_ok=True)
with pd.ExcelWriter("output/dashboard_data.xlsx", engine='xlsxwriter') as writer:
    analytics_dashboard.to_excel(writer, sheet_name="Active_Analytics", index=False)
    audit_trail.to_excel(writer, sheet_name="Full_Audit_Trail", index=False)

print("✅ Dashboard ready.")
