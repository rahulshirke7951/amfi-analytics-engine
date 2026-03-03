import sqlite3
import pandas as pd
import requests
import json
import os
import gdown
import re
from datetime import datetime, timedelta

# ==========================
# LOAD CONFIG & DOWNLOADS
# ==========================
with open("config.json", "r") as f:
    config = json.load(f)

gdown.download(config["historic_db_url"], "historic.db", quiet=True, fuzzy=True)

release_info = requests.get(config["mf_release_api"]).json()
asset_url = next(a["browser_download_url"] for a in release_info["assets"] if a["name"] == "mf.db")
with open("mf.db", "wb") as f: 
    f.write(requests.get(asset_url).content)

# ==========================
# DATA LOADING & STRICT PARSING
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
    """Priority: ISO (YYYY-MM-DD), then Indian (DD-MM-YYYY)"""
    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%Y/%m/%d'):
        try:
            return pd.to_datetime(date_str, format=fmt)
        except (ValueError, TypeError):
            continue
    return pd.to_datetime(date_str, errors='coerce')

df["nav_date"] = df["nav_date"].apply(strict_date_parse)
df = df.dropna(subset=['nav_date']).sort_values(["scheme_code", "nav_date"])

# Safety: Remove future dates
today = datetime.now()
df = df[df["nav_date"] <= (today + timedelta(days=1))]

# ==========================
# ANALYTICS & BIFURCATION
# ==========================
history_counts = df.groupby("scheme_code").size().rename("history_count")
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
meta['plan_type'] = meta['scheme_name'].apply(lambda x: 'Direct Plan' if 'Direct' in str(x) else ('Regular Plan' if 'Regular' in str(x) else 'NA'))
meta['payout_option'] = meta['scheme_name'].apply(lambda x: 'Growth' if 'Growth' in str(x) else ('IDCW' if any(i in str(x) for i in ['IDCW', 'Dividend']) else 'NA'))

# Returns Calculation
latest_date = df["nav_date"].max()
latest_nav_df = df.sort_values("nav_date").groupby("scheme_code").tail(1).copy()
analytics = latest_nav_df[['scheme_code', 'nav', 'nav_date']].rename(columns={'nav': 'latest_nav', 'nav_date': 'latest_nav_date'})

for d in config["return_periods_days"]:
    cutoff = latest_date - timedelta(days=d)
    past = df[df["nav_date"] <= cutoff].sort_values("nav_date").groupby("scheme_code").tail(1).copy()
    past = past[['scheme_code', 'nav']].rename(columns={'nav': f'nav_{d}d'})
    analytics = analytics.merge(past, on='scheme_code', how='left')
    analytics[f'return_{d}d'] = (analytics['latest_nav'] - analytics[f'nav_{d}d']) / analytics[f'nav_{d}d'] * 100

# Anchor Date
anchor_dt = pd.to_datetime(config["anchor_date"])
anchor_df = df[df["nav_date"] <= anchor_dt].sort_values("nav_date").groupby("scheme_code").tail(1).copy()
analytics = analytics.merge(anchor_df[['scheme_code', 'nav']].rename(columns={'nav': 'nav_anchor'}), on='scheme_code', how='left')
analytics['return_since_anchor'] = (analytics['latest_nav'] - analytics['nav_anchor']) / analytics['nav_anchor'] * 100

# Final Save
analytics = analytics.merge(meta, on="scheme_code", how="left").merge(history_counts, on="scheme_code", how="left")
os.makedirs("output", exist_ok=True)
analytics.to_excel("output/dashboard_data.xlsx", index=False)
print("✅ Main Dashboard ready with Strict Parsing.")
