import sqlite3
import pandas as pd
import requests
import json
import os
import re
import logging
import gdown
from datetime import timedelta

# ==========================
# LOGGING
# ==========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ==========================
# 1. LOAD CONFIG
# ==========================
with open("config.json", "r") as f:
    config = json.load(f)

scheme_code = str(config.get("audit_scheme_code", 140088))
log.info(f"🔎 Running FULL Production Logic (Single Scheme) → {scheme_code}")

# ==========================
# 2. DOWNLOAD DATABASES
# ==========================
if not os.path.exists("historic.db"):
    log.info("Downloading historic.db...")
    gdown.download(config["historic_db_url"], "historic.db", quiet=True, fuzzy=True)

log.info("Downloading daily mf.db...")
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

# ==========================
# 3. LOAD NAV DATA
# ==========================
with sqlite3.connect(":memory:") as conn:
    conn.execute("ATTACH DATABASE 'mf.db' AS daily;")
    conn.execute("ATTACH DATABASE 'historic.db' AS historic;")

    query = f"""
        SELECT scheme_code, nav, nav_date, 'daily' AS source
        FROM daily.nav_history
        WHERE scheme_code = '{scheme_code}'

        UNION ALL

        SELECT scheme_code, nav_value AS nav, nav_date, 'historic' AS source
        FROM historic.nav_history
        WHERE scheme_code = '{scheme_code}'
    """
    df = pd.read_sql_query(query, conn)

# ==========================
# 4. CLEAN & DEDUPE
# ==========================
df["nav_date"] = pd.to_datetime(df["nav_date"], errors="coerce")
df = df.dropna(subset=["nav_date"])

df = (
    df.sort_values(["nav_date", "source"])
      .drop_duplicates(["scheme_code", "nav_date"], keep="first")
)

if df.empty:
    raise RuntimeError(f"No NAV data found for scheme {scheme_code}")

# ==========================
# 5. ANCHOR & FRESHNESS
# ==========================
latest_row = df.sort_values("nav_date").iloc[-1]
latest_nav_value = latest_row["nav"]
latest_nav_date_value = pd.Timestamp(latest_row["nav_date"])

latest_nav_date = df["nav_date"].max()
today = (latest_nav_date - timedelta(days=1)).replace(
    hour=0, minute=0, second=0, microsecond=0
)

freshness_days = config.get("freshness_threshold_days", 5)
freshness_threshold = today - timedelta(days=freshness_days)

if latest_nav_date_value < freshness_threshold:
    raise RuntimeError("Scheme excluded due to stale NAV data.")

log.info(f"Anchor Date: {today.date()} (Latest NAV: {latest_nav_date_value.date()})")

# ==========================
# 6. REINDEX + FORWARD FILL
# ==========================
max_period = max(config["return_periods_days"])
buffer_days = config.get("reindex_buffer_days", 15)

reindex_start = today - timedelta(days=max_period + buffer_days)
df_active = df[df["nav_date"] >= reindex_start].copy()

all_dates = pd.date_range(df_active["nav_date"].min(), today, freq="D")

df_filled = (
    df_active.set_index("nav_date")
             .reindex(all_dates)
             .ffill()
             .reset_index()
             .rename(columns={"index": "nav_date"})
)

# ==========================
# 7. COMPUTE RETURNS
# ==========================
results = []

for d in config["return_periods_days"]:
    target_date = today - timedelta(days=d)
    available = df_filled["nav_date"][df_filled["nav_date"] <= target_date]

    if available.empty:
        past_nav = None
    else:
        past_nav = df_filled.loc[
            df_filled["nav_date"] == available.iloc[-1], "nav"
        ].values[0]

    return_pct = (
        round((latest_nav_value - past_nav) / past_nav * 100, 2)
        if past_nav and past_nav > 0
        else None
    )

    results.append({
        "scheme_code": scheme_code,
        "latest_nav_date": latest_nav_date_value.date(),
        "latest_nav": latest_nav_value,
        "period_days": d,
        "start_nav": past_nav,
        "return_pct": return_pct
    })

returns_df = pd.DataFrame(results)

# ==========================
# 8. FETCH METADATA (Daily Only)
# ==========================
with sqlite3.connect("mf.db") as conn:
    meta_query = f"""
        SELECT scheme_code, scheme_name, amc_name, scheme_category
        FROM nav_history
        WHERE scheme_code = '{scheme_code}'
        ORDER BY nav_date DESC
        LIMIT 1
    """
    meta = pd.read_sql_query(meta_query, conn)

# Metadata Parsing
_CAT_PATTERN = re.compile(r'^(.*?)\s*\(\s*(.*?)\s*\)$')
_PLAN_PATTERN = re.compile(r'\b(Direct|Regular)\b', re.IGNORECASE)
_OPTION_PATTERN = re.compile(r'\b(IDCW|Dividend|Bonus|Growth)\b', re.IGNORECASE)

def split_category(cat_str):
    if not isinstance(cat_str, str):
        return pd.Series(["NA","NA","NA"])
    m = _CAT_PATTERN.match(cat_str.strip())
    if not m:
        return pd.Series([cat_str.strip(),"NA","NA"])
    main = m.group(1).strip()
    parts = [p.strip() for p in m.group(2).split(" - ")]
    return pd.Series([
        main,
        parts[0] if len(parts)>0 else "NA",
        parts[1] if len(parts)>1 else "NA"
    ])

meta[["cat_level_1","cat_level_2","cat_level_3"]] = \
    meta["scheme_category"].apply(split_category)

meta["plan_type"] = meta["scheme_name"].apply(
    lambda x: (m:=_PLAN_PATTERN.search(str(x))) and m.group(1).capitalize() or "Regular"
)

meta["option_type"] = meta["scheme_name"].apply(
    lambda x: (m:=_OPTION_PATTERN.search(str(x))) and 
    m.group(1).upper().replace("DIVIDEND","IDCW").capitalize() or "Growth"
)

# ==========================
# 9. EXPORT
# ==========================
os.makedirs("output", exist_ok=True)

output_file = "output/single_scheme_production_equivalent.xlsx"

with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
    df_filled.to_excel(writer, sheet_name="Reindexed_NAV", index=False)
    returns_df.to_excel(writer, sheet_name="Returns", index=False)
    meta.to_excel(writer, sheet_name="Metadata", index=False)

log.info(f"✅ Process Complete. Output saved to {output_file}")
