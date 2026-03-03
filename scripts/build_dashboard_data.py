import sqlite3
import pandas as pd
import requests
from io import BytesIO
import os
from datetime import datetime, timedelta

# ==========================
# CONFIG
# ==========================

HISTORIC_DB_URL = "PASTE_DRIVE_DIRECT_DOWNLOAD_LINK_HERE"
MF_RELEASE_API = "https://api.github.com/repos/YOUR_USERNAME/YOUR_NAV_REPO/releases/latest"

ANCHOR_DATE = "2026-02-20"
OUTPUT_FILE = "output/dashboard_data.xlsx"

os.makedirs("output", exist_ok=True)

# ==========================
# Download historic.db
# ==========================

def download_file(url, filename):
    response = requests.get(url)
    response.raise_for_status()
    with open(filename, "wb") as f:
        f.write(response.content)

print("Downloading historic.db...")
download_file(HISTORIC_DB_URL, "historic.db")

# ==========================
# Download latest mf.db from Release
# ==========================

print("Fetching latest mf.db release info...")
release_info = requests.get(MF_RELEASE_API).json()

asset_url = None
for asset in release_info.get("assets", []):
    if asset["name"] == "mf.db":
        asset_url = asset["browser_download_url"]
        break

if not asset_url:
    raise Exception("mf.db not found in latest release")

print("Downloading mf.db...")
download_file(asset_url, "mf.db")

# ==========================
# Attach both databases
# ==========================

conn = sqlite3.connect(":memory:")
conn.execute("ATTACH DATABASE 'mf.db' AS daily;")
conn.execute("ATTACH DATABASE 'historic.db' AS historic;")

# ==========================
# Build unified NAV dataset
# ==========================

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
# Return Calculation
# ==========================

def calculate_return(df, days):
    cutoff = df["nav_date"].max() - timedelta(days=days)
    past = df[df["nav_date"] <= cutoff].groupby("scheme_code").last()["nav"]
    latest = df.groupby("scheme_code").last()["nav"]
    return ((latest - past) / past * 100).rename(f"return_{days}d")

print("Calculating returns...")

latest_nav = df.groupby("scheme_code").last()[["nav"]]
latest_nav.rename(columns={"nav": "latest_nav"}, inplace=True)

r_30 = calculate_return(df, 30)
r_90 = calculate_return(df, 90)
r_180 = calculate_return(df, 180)
r_365 = calculate_return(df, 365)

# Since Anchor
anchor = pd.to_datetime(ANCHOR_DATE)
anchor_nav = df[df["nav_date"] <= anchor].groupby("scheme_code").last()["nav"]
since_anchor = ((latest_nav["latest_nav"] - anchor_nav) / anchor_nav * 100).rename("return_since_anchor")

# Combine
final = latest_nav.join([r_30, r_90, r_180, r_365, since_anchor])

# ==========================
# Merge Metadata (from daily)
# ==========================

meta_query = """
SELECT DISTINCT scheme_code, scheme_name, amc_name, scheme_category
FROM daily.nav_history
"""

meta = pd.read_sql_query(meta_query, conn)

final = final.merge(meta, on="scheme_code", how="left")

# Reorder columns
final = final.reset_index()
final = final[
    [
        "scheme_code",
        "scheme_name",
        "amc_name",
        "scheme_category",
        "latest_nav",
        "return_30d",
        "return_90d",
        "return_180d",
        "return_365d",
        "return_since_anchor"
    ]
]

# Save
final.to_excel(OUTPUT_FILE, index=False)

print("✅ Dashboard data created:", OUTPUT_FILE)
