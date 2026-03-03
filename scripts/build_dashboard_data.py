import sqlite3
import pandas as pd
import requests
import json
import os
import gdown
import re
from datetime import timedelta

# ==========================
# LOAD CONFIG & DOWNLOADS
# ==========================
with open("config.json", "r") as f:
    config = json.load(f)

print("Downloading databases...")
gdown.download(config["historic_db_url"], "historic.db", quiet=False, fuzzy=True)

release_info = requests.get(config["mf_release_api"]).json()
asset_url = next(a["browser_download_url"] for a in release_info["assets"] if a["name"] == "mf.db")
with open("mf.db", "wb") as f: 
    f.write(requests.get(asset_url).content)

# ==========================
# DATA LOADING
# ==========================
conn = sqlite3.connect(":memory:")
conn.execute("ATTACH DATABASE 'mf.db' AS daily;")
conn.execute("ATTACH DATABASE 'historic.db' AS historic;")

print("Fetching NAV history...")
query = """
SELECT scheme_code, nav, nav_date FROM daily.nav_history
UNION
SELECT scheme_code, nav_value AS nav, nav_date FROM historic.nav_history
"""
df = pd.read_sql_query(query, conn)
df["nav_date"] = pd.to_datetime(df["nav_date"], format='mixed', dayfirst=True)
df = df.sort_values(["scheme_code", "nav_date"])

# History Count per scheme (Data Quality Check)
history_counts = df.groupby("scheme_code").size().rename("history_count")

# Load Metadata (Latest info is Source of Truth)
print("Fetching Master Metadata...")
meta = pd.read_sql_query("SELECT DISTINCT scheme_code, scheme_name, amc_name, scheme_category FROM daily.nav_history", conn)

# ==========================
# BIFURCATION (Smart Filters)
# ==========================
def split_category(cat_str):
    if not cat_str or '(' not in cat_str:
        return pd.Series([cat_str or 'NA', 'NA', 'NA'])
    main = cat_str.split('(')[0].strip()
    sub_part = re.search(r'\((.*?)\)', cat_str)
    if sub_part:
        sub_content = sub_part.group(1).split(' - ')
        sub1 = sub_content[0].strip() if len(sub_content) > 0 else 'NA'
        sub2 = sub_content[1].strip() if len(sub_content) > 1 else 'NA'
        return pd.Series([main, sub1, sub2])
    return pd.Series([main, 'NA', 'NA'])

meta[['cat_level_1', 'cat_level_2', 'cat_level_3']] = meta['scheme_category'].apply(split_category)

# Extract Plan/Option from Scheme Name
meta['plan_type'] = 'NA'
meta.loc[meta['scheme_name'].str.contains('Direct', case=False, na=False), 'plan_type'] = 'Direct Plan'
meta.loc[meta['scheme_name'].str.contains('Regular', case=False, na=False), 'plan_type'] = 'Regular Plan'

meta['payout_option'] = 'NA'
meta.loc[meta['scheme_name'].str.contains('Growth', case=False, na=False), 'payout_option'] = 'Growth'
meta.loc[meta['scheme_name'].str.contains('IDCW|Dividend', case=False, na=False), 'payout_option'] = 'IDCW'

# ==========================
# ANALYTICS & PIVOTED AUDIT
# ==========================
print("Calculating Analytics and Pivoted Audit Trail...")
latest_date = df["nav_date"].max()
latest_nav_df = df.groupby("scheme_code").tail(1).copy()

analytics = latest_nav_df[['scheme_code', 'nav', 'nav_date']].rename(
    columns={'nav': 'latest_nav', 'nav_date': 'latest_nav_date'}
)

audit_pivoted = latest_nav_df[['scheme_code', 'nav_date', 'nav']].rename(
    columns={'nav_date': 'latest_date', 'nav': 'latest_nav'}
)

def get_period_data(days, label):
    cutoff = latest_date - timedelta(days=days)
    past = df[df["nav_date"] <= cutoff].groupby("scheme_code").tail(1).copy()
    return past[['scheme_code', 'nav_date', 'nav']].rename(
        columns={'nav_date': f'{label}_date', 'nav': f'{label}_nav'}
    )

for d in config["return_periods_days"]:
    period_df = get_period_data(d, f"{d}d")
    audit_pivoted = audit_pivoted.merge(period_df, on='scheme_code', how='left')
    nav_col = f"{d}d_nav"
    analytics[f'return_{d}d'] = ((analytics['latest_nav'] - audit_pivoted[nav_col]) / audit_pivoted[nav_col] * 100)

anchor_dt = pd.to_datetime(config["anchor_date"])
anchor_df = df[df["nav_date"] <= anchor_dt].groupby("scheme_code").tail(1).copy()
anchor_df = anchor_df[['scheme_code', 'nav_date', 'nav']].rename(
    columns={'nav_date': 'anchor_date', 'nav': 'anchor_nav'}
)
audit_pivoted = audit_pivoted.merge(anchor_df, on='scheme_code', how='left')
analytics['return_since_anchor'] = ((analytics['latest_nav'] - audit_pivoted['anchor_nav']) / audit_pivoted['anchor_nav'] * 100)

# ==========================
# FINAL MERGE & SAVE
# ==========================
analytics = analytics.merge(meta, on="scheme_code", how="left")
analytics = analytics.merge(history_counts, on="scheme_code", how="left")

# Reorder columns
cols = [
    'scheme_code', 'scheme_name', 'amc_name', 'scheme_category',
    'cat_level_1', 'cat_level_2', 'cat_level_3', 'plan_type', 'payout_option',
    'history_count', 'latest_nav', 'latest_nav_date'
]
cols += [f'return_{d}d' for d in config["return_periods_days"]] + ['return_since_anchor']
analytics = analytics[cols]

os.makedirs("output", exist_ok=True)
with pd.ExcelWriter("output/dashboard_data.xlsx", engine='openpyxl') as writer:
    analytics.to_excel(writer, sheet_name="Analytics_Dashboard", index=False)
    audit_pivoted.to_excel(writer, sheet_name="Audit_Trail", index=False)

print("✅ Dashboard ready.")
