import sqlite3
import pandas as pd
import requests
import json
import os
import gdown
from datetime import timedelta

# ==========================
# LOAD CONFIG
# ==========================

with open("config.json", "r") as f:
    config = json.load(f)

HISTORIC_DB_URL = config["historic_db_url"]
MF_RELEASE_API = config["mf_release_api"]
ANCHOR_DATE = config["anchor_date"]
RETURN_PERIODS = config["return_periods_days"]

OUTPUT_FILE = "output/dashboard_data.xlsx"
os.makedirs("output", exist_ok=True)

# ==========================
# DOWNLOAD HISTORIC DB (Drive via gdown)
# ==========================

print("Downloading historic.db using gdown...")
gdown.download(HISTORIC_DB_URL, "historic.db", quiet=False)

# ==========================
# DOWNLOAD LATEST MF.DB FROM RELEASE
# ==========================

print("Fetching latest release info...")

release_info = requests.get(MF_RELEASE_API).json()

asset_url = None
for asset in release_info.get("assets", []):
    if asset["name"] == "mf.db":
        asset_url = asset["browser_download_url"]
        break

if not asset_url:
    raise Exception("mf.db not found in latest release")

print("Downloading mf.db...")
response = requests.get(asset_url)
response.raise_for_status()

with open("mf.db", "wb") as f:
    f.write(response.content)

# ==========================
# ATTACH BOTH DATABASES
# ==========================

print("Attaching databases...")

conn = sqlite3.connect(":memory:")
conn.execute("ATTACH DATABASE 'mf.db' AS daily;")
conn.execute("ATTACH DATABASE 'historic.db' AS historic;")

# ==========================
# UNIFIED NAV DATASET
# ==========================

print("Building unified NAV dataset...")

query = """
SELECT scheme_code, nav, nav_date
FROM daily.nav_history

UNION

SELECT scheme_code, nav_value AS nav, nav_date
FROM historic.nav_history
"""

df = pd.read_sql_query(query, conn)

df["nav_date"] = pd.to_datetime(df["nav_date"])
df = df.sort_values(["scheme_code", "nav_date"])

# ==========================
# CALCULATE RETURNS
# ==========================

print("Calculating returns...")

latest_nav = df.groupby("scheme_code").last()[["nav"]]
latest_nav.rename(columns={"nav": "latest_nav"}, inplace=True)

returns = []

for days in RETURN_PERIODS:
    cutoff = df["nav_date"].max() - timedelta(days=days)
    past = df[df["nav_date"] <= cutoff].groupby("scheme_code").last()["nav"]
    latest = latest_nav["latest_nav"]
    r = ((latest - past) / past * 100).rename(f"return_{days}d")
    returns.append(r)

# Since Anchor
anchor = pd.to_datetime(ANCHOR_DATE)
anchor_nav = df[df["nav_date"] <= anchor].groupby("scheme_code").last()["nav"]
since_anchor = ((latest_nav["latest_nav"] - anchor_nav) / anchor_nav * 100).rename("return_since_anchor")

final = latest_nav.join(returns + [since_anchor])

# ==========================
# MERGE METADATA
# ==========================

print("Merging metadata...")

meta_query = """
SELECT DISTINCT scheme_code, scheme_name, amc_name, scheme_category
FROM daily.nav_history
"""

meta = pd.read_sql_query(meta_query, conn)

final = final.merge(meta, on="scheme_code", how="left")
final = final.reset_index()

# Reorder columns
final = final[
    [
        "scheme_code",
        "scheme_name",
        "amc_name",
        "scheme_category",
        "latest_nav",
    ]
    + [f"return_{d}d" for d in RETURN_PERIODS]
    + ["return_since_anchor"]
]

# ==========================
# SAVE OUTPUT
# ==========================

print("Saving dashboard file...")

final.to_excel(OUTPUT_FILE, index=False)

print("✅ Dashboard data created successfully.")
