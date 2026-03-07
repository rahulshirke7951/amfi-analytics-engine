import sqlite3
import pandas as pd
import json
import os
import logging
import gdown
import sys
from datetime import datetime

# ==========================
# LOGGING
# ==========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

if len(sys.argv) < 2:
    print("Usage: python debug_scheme_extract.py <scheme_code>")
    sys.exit(1)

scheme_code_input = sys.argv[1]

# ==========================
# LOAD CONFIG
# ==========================
with open("config.json", "r") as f:
    config = json.load(f)

# ==========================
# DOWNLOAD IF NEEDED
# ==========================
if not os.path.exists("historic.db"):
    log.info("Downloading historic.db...")
    gdown.download(config["historic_db_url"], "historic.db", quiet=False, fuzzy=True)

if not os.path.exists("mf.db"):
    raise RuntimeError("mf.db not found. Run dashboard script first.")

# ==========================
# DATE PARSER (SAME AS PROD)
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
    print("❌ No data found for this scheme code.")
    sys.exit(0)

# ==========================
# APPLY SAME CLEANING
# ==========================
df["parsed_nav_date"] = parse_dates_vectorized(df["nav_date"])

df = df.sort_values(["parsed_nav_date", "source"])
df = df.drop_duplicates(["parsed_nav_date"], keep="first")

df = df.sort_values("parsed_nav_date")

print("\n✅ Extracted Records:", len(df))
print("Date Range:", df["parsed_nav_date"].min(), "→", df["parsed_nav_date"].max())

# ==========================
# EXPORT
# ==========================
os.makedirs("debug_output", exist_ok=True)
file_path = f"debug_output/debug_{scheme_code_input}.xlsx"

df.to_excel(file_path, index=False)

print(f"\n📂 Debug file saved to: {file_path}")
