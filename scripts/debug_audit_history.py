import sqlite3
import pandas as pd
import requests
import json
import os
import logging
import gdown
import sys

# ==========================
# LOGGING
# ==========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ==========================
# READ SCHEME CODE
# ==========================
if len(sys.argv) < 2:
    raise RuntimeError("Scheme code required. Example: python debug_audit_history.py 140088")

scheme_code_input = sys.argv[1]
log.info(f"Debugging Scheme Code: {scheme_code_input}")

# ==========================
# LOAD CONFIG
# ==========================
with open("config.json", "r") as f:
    config = json.load(f)

# ==========================
# DOWNLOAD historic.db
# ==========================
if not os.path.exists("historic.db"):
    log.info("Downloading historic.db...")
    gdown.download(config["historic_db_url"], "historic.db", quiet=False, fuzzy=True)

# ==========================
# DOWNLOAD mf.db
# ==========================
log.info("Downloading mf.db from GitHub release...")

response = requests.get(config["mf_release_api"], timeout=30)
release_info = response.json()

asset_url = next(
    (a["browser_download_url"] for a in release_info["assets"] if a["name"] == "mf.db"),
    None
)

if not asset_url:
    raise RuntimeError("mf.db not found in release assets.")

with open("mf.db", "wb") as f:
    f.write(requests.get(asset_url, timeout=60).content)

log.info("mf.db downloaded successfully.")

# ==========================
# EXTRACT RAW DATA SEPARATELY
# ==========================
log.info("Extracting RAW Daily and Historic data...")

with sqlite3.connect(":memory:") as conn:
    conn.execute("ATTACH DATABASE 'mf.db' AS daily;")
    conn.execute("ATTACH DATABASE 'historic.db' AS historic;")

    daily_df = pd.read_sql_query(f"""
        SELECT *
        FROM daily.nav_history
        WHERE scheme_code = '{scheme_code_input}'
        ORDER BY nav_date
    """, conn)

    historic_df = pd.read_sql_query(f"""
        SELECT *
        FROM historic.nav_history
        WHERE scheme_code = '{scheme_code_input}'
        ORDER BY nav_date
    """, conn)

# Add source labels
daily_df["source"] = "daily"
historic_df["source"] = "historic"

merged_raw = pd.concat([daily_df, historic_df], ignore_index=True)

log.info(f"Daily Records: {len(daily_df)}")
log.info(f"Historic Records: {len(historic_df)}")

# ==========================
# EXPORT WITHOUT ANY CLEANING
# ==========================
os.makedirs("debug_output", exist_ok=True)
file_path = f"debug_output/raw_debug_{scheme_code_input}.xlsx"

with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
    daily_df.to_excel(writer, sheet_name="Raw_Daily_DB", index=False)
    historic_df.to_excel(writer, sheet_name="Raw_Historic_DB", index=False)
    merged_raw.to_excel(writer, sheet_name="Merged_No_Parsing", index=False)

log.info(f"Raw debug file saved to: {file_path}")
log.info("Debug completed successfully.")
