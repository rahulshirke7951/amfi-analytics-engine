import sqlite3
import pandas as pd
import requests
import json
import os
import logging
import gdown
import sys
from datetime import datetime

# ==========================
# LOGGING
# ==========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ==========================
# READ SCHEME CODE ARGUMENT
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
else:
    log.info("Using cached historic.db")

# ==========================
# DOWNLOAD mf.db FROM RELEASE
# ==========================
log.info("Downloading mf.db from GitHub release...")

try:
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

except Exception as e:
    raise RuntimeError(f"Failed to download mf.db: {e}") from e

# ==========================
# DATE PARSER (SAME AS DASHBOARD)
# ==========================
def parse_dates_vectorized(series: pd.Series) -> pd.Series:
    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%Y/%m/%d'):
        parsed = pd.to_datetime(series, format=fmt, errors='coerce')
        if parsed.notna().mean() > 0.95:
            return parsed
    return pd.to_datetime(series, errors='coerce')

# ==========================
# LOAD ONLY ONE SCHEME
# ==========================
log.info("Loading scheme data from both databases...")

with sqlite3.connect(":memory:") as conn:
    conn.execute("ATTACH DATABASE 'mf.db' AS daily;")
    conn.execute("ATTACH DATABASE 'historic.db' AS historic;")

    query = f"""
        SELECT scheme_code, nav, nav_date, 'daily' AS source
        FROM daily.nav_history
        WHERE scheme_code = '{scheme_code_input}'

        UNION ALL

        SELECT scheme_code, nav_value AS nav, nav_date, 'historic' AS source
        FROM historic.nav_history
        WHERE scheme_code = '{scheme_code_input}'
    """

    df = pd.read_sql_query(query, conn)

if df.empty:
    log.warning("No data found for this scheme.")
    sys.exit(0)

# ==========================
# APPLY SAME CLEANING LOGIC
# ==========================
log.info("Parsing and deduplicating dates...")

df["parsed_nav_date"] = parse_dates_vectorized(df["nav_date"])
df = df.dropna(subset=["parsed_nav_date"])

df = (
    df.sort_values(["parsed_nav_date", "source"])
      .drop_duplicates(["parsed_nav_date"], keep="first")
      .sort_values("parsed_nav_date")
)

# ==========================
# SUMMARY LOGS
# ==========================
log.info(f"Total Records After Merge: {len(df)}")
log.info(f"Date Range: {df['parsed_nav_date'].min()} → {df['parsed_nav_date'].max()}")

# ==========================
# EXPORT DEBUG FILE
# ==========================
os.makedirs("debug_output", exist_ok=True)
file_path = f"debug_output/debug_{scheme_code_input}.xlsx"

df.to_excel(file_path, index=False)

log.info(f"Debug file saved to: {file_path}")
log.info("Debug run completed successfully.")
