import sqlite3
import pandas as pd
import requests
import json
import os
import re
import logging
import gdown
from datetime import datetime, timedelta

# ==========================
# LOGGING SETUP
# ==========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ==========================
# DEBUG CONFIGURATION
# ==========================
DEBUG_SCHEME_CODE = "140088"
log.info(f"🔍 DEBUG MODE: Tracing scheme_code = {DEBUG_SCHEME_CODE}")

# ==========================
# 1. LOAD CONFIG & DOWNLOADS
# ==========================
with open("config.json", "r") as f:
    config = json.load(f)

# Historic DB (cached)
if not os.path.exists("historic.db"):
    log.info("Cache miss: Downloading historic database...")
    gdown.download(config["historic_db_url"], "historic.db", quiet=True, fuzzy=True)
else:
    log.info("Cache hit: Using cached historic.db")

# Daily mf.db
log.info("Fetching daily mf.db from GitHub API...")
try:
    response = requests.get(config["mf_release_api"], timeout=30)
    release_info = response.json()
    asset_url = next(
        (a["browser_download_url"] for a in release_info["assets"] if a["name"] == "mf.db"),
        None
    )
    if not asset_url:
        raise RuntimeError("mf.db not found among release assets.")
    
    with open("mf.db", "wb") as f:
        f.write(requests.get(asset_url, timeout=60).content)
    log.info("mf.db downloaded successfully.")
except Exception as e:
    raise RuntimeError(f"Failed to download mf.db: {e}") from e

# ==========================
# 2. DATA LOADING - WITH DEBUG TRACKING
# ==========================

def parse_dates_vectorized(series: pd.Series) -> pd.Series:
    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%Y/%m/%d'):
        parsed = pd.to_datetime(series, format=fmt, errors='coerce')
        if parsed.notna().mean() > 0.95:
            return parsed
    return pd.to_datetime(series, errors='coerce')

# Create debug dictionary to store all intermediate results
debug_data = {}

with sqlite3.connect(":memory:") as conn:
    conn.execute("ATTACH DATABASE 'mf.db' AS daily;")
    conn.execute("ATTACH DATABASE 'historic.db' AS historic;")

    # DEBUG STEP 1: Check raw data from both sources
    log.info("=" * 80)
    log.info("STEP 1: RAW DATA FROM SOURCES")
    log.info("=" * 80)
    
    daily_query = f"SELECT * FROM daily.nav_history WHERE scheme_code = '{DEBUG_SCHEME_CODE}'"
    historic_query = f"SELECT scheme_code, nav_value AS nav, nav_date FROM historic.nav_history WHERE scheme_code = '{DEBUG_SCHEME_CODE}'"
    
    daily_raw = pd.read_sql_query(daily_query, conn)
    historic_raw = pd.read_sql_query(historic_query, conn)
    
    log.info(f"Daily DB: {len(daily_raw)} records found")
    log.info(f"Historic DB: {len(historic_raw)} records found")
    
    debug_data['1_daily_raw'] = daily_raw.copy()
    debug_data['1_historic_raw'] = historic_raw.copy()
    
    if len(daily_raw) > 0:
        log.info(f"Daily date range: {daily_raw['nav_date'].min()} to {daily_raw['nav_date'].max()}")
    if len(historic_raw) > 0:
        log.info(f"Historic date range: {historic_raw['nav_date'].min()} to {historic_raw['nav_date'].max()}")

    # UNION ALL both sources
    query = """
        SELECT scheme_code, nav, nav_date, 'daily' AS source FROM daily.nav_history
        UNION ALL
        SELECT scheme_code, nav_value AS nav, nav_date, 'historic' AS source FROM historic.nav_history
    """
    df = pd.read_sql_query(query, conn)
    
    # Metadata
    meta_query = """
        SELECT scheme_code, scheme_name, amc_name, scheme_category
        FROM (
            SELECT scheme_code, scheme_name, amc_name, scheme_category,
                   ROW_NUMBER() OVER (PARTITION BY scheme_code ORDER BY nav_date DESC) as rn
            FROM daily.nav_history
        ) WHERE rn = 1
    """
    meta_raw = pd.read_sql_query(meta_query, conn)

# Filter debug scheme from full dataset
df_debug = df[df["scheme_code"] == DEBUG_SCHEME_CODE].copy()
debug_data['2_union_raw'] = df_debug.copy()

log.info("=" * 80)
log.info("STEP 2: AFTER UNION (Before Date Parsing)")
log.info("=" * 80)
log.info(f"Total records after UNION: {len(df_debug)}")
log.info(f"Source breakdown:\n{df_debug['source'].value_counts()}")

# DEBUG STEP 3: Date Parsing
log.info("=" * 80)
log.info("STEP 3: DATE PARSING")
log.info("=" * 80)

df["nav_date"] = parse_dates_vectorized(df["nav_date"])
df_debug = df[df["scheme_code"] == DEBUG_SCHEME_CODE].copy()

log.info(f"Records before dropna(nav_date): {len(df_debug)}")
df = df.dropna(subset=["nav_date"])
df_debug = df[df["scheme_code"] == DEBUG_SCHEME_CODE].copy()
log.info(f"Records after dropna(nav_date): {len(df_debug)}")

debug_data['3_after_date_parse'] = df_debug.copy()

# DEBUG STEP 4: Deduplication
log.info("=" * 80)
log.info("STEP 4: DEDUPLICATION (daily source wins)")
log.info("=" * 80)

# Check for duplicates before dedup
duplicates = df_debug[df_debug.duplicated(["scheme_code", "nav_date"], keep=False)]
if len(duplicates) > 0:
    log.info(f"Found {len(duplicates)} duplicate date entries")
    debug_data['4_duplicates_before'] = duplicates.sort_values(['nav_date', 'source'])

df = (
    df.sort_values(["scheme_code", "nav_date", "source"])
      .drop_duplicates(["scheme_code", "nav_date"], keep="first")
      .drop(columns=["source"])
)
df_debug = df[df["scheme_code"] == DEBUG_SCHEME_CODE].copy()
log.info(f"Records after deduplication: {len(df_debug)}")

debug_data['4_after_dedup'] = df_debug.copy()

# DEBUG STEP 5: Anchor Date
log.info("=" * 80)
log.info("STEP 5: ANCHOR DATE DERIVATION")
log.info("=" * 80)

latest_nav_date = df["nav_date"].max()
if pd.isna(latest_nav_date):
    raise RuntimeError("No NAV data available.")

today = (latest_nav_date - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
log.info(f"Latest NAV date in dataset: {latest_nav_date.date()}")
log.info(f"Anchor Date (today): {today.date()}")

scheme_latest = df_debug['nav_date'].max()
log.info(f"Scheme {DEBUG_SCHEME_CODE} latest NAV date: {scheme_latest.date() if pd.notna(scheme_latest) else 'N/A'}")

# DEBUG STEP 6: Status & Freshness Check
log.info("=" * 80)
log.info("STEP 6: STATUS & FRESHNESS CHECK")
log.info("=" * 80)

max_period = max(config["return_periods_days"])
buffer_days = config.get("reindex_buffer_days", 15)
freshness_days = config.get("freshness_threshold_days", 5)

reindex_start = today - timedelta(days=max_period + buffer_days)
freshness_threshold = today - timedelta(days=freshness_days)

log.info(f"Max return period: {max_period} days")
log.info(f"Buffer days: {buffer_days}")
log.info(f"Reindex start date: {reindex_start.date()}")
log.info(f"Freshness threshold: {freshness_threshold.date()}")

latest_nav_df = df.sort_values("nav_date").groupby("scheme_code").tail(1).copy()
scheme_latest_record = latest_nav_df[latest_nav_df["scheme_code"] == DEBUG_SCHEME_CODE]

if len(scheme_latest_record) > 0:
    latest_date = scheme_latest_record.iloc[0]["nav_date"]
    latest_nav = scheme_latest_record.iloc[0]["nav"]
    status = "Active" if latest_date >= freshness_threshold else "Excluded: Stale Data"
    log.info(f"Scheme {DEBUG_SCHEME_CODE}:")
    log.info(f"  Latest NAV Date: {latest_date.date()}")
    log.info(f"  Latest NAV: {latest_nav}")
    log.info(f"  Status: {status}")
    log.info(f"  Days since last update: {(today - latest_date).days}")
else:
    log.warning(f"Scheme {DEBUG_SCHEME_CODE} NOT FOUND in latest_nav_df")

debug_data['6_latest_record'] = scheme_latest_record.copy()

# DEBUG STEP 7: Reindexing
log.info("=" * 80)
log.info("STEP 7: REINDEXING (Forward Fill)")
log.info("=" * 80)

audit_trail = latest_nav_df[["scheme_code", "nav_date", "nav"]].rename(
    columns={"nav_date": "latest_nav_date", "nav": "latest_nav"}
)
audit_trail["status"] = audit_trail["latest_nav_date"].apply(
    lambda x: "Active" if x >= freshness_threshold else "Excluded: Stale Data"
)

active_codes = audit_trail[audit_trail["status"] == "Active"]["scheme_code"].unique()
log.info(f"Total active schemes: {len(active_codes)}")
log.info(f"Is {DEBUG_SCHEME_CODE} active? {DEBUG_SCHEME_CODE in active_codes}")

df_active = df[df["scheme_code"].isin(active_codes) & (df["nav_date"] >= reindex_start)].copy()
df_debug_active = df_active[df_active["scheme_code"] == DEBUG_SCHEME_CODE].copy()

log.info(f"Records for {DEBUG_SCHEME_CODE} in active set (after date filter): {len(df_debug_active)}")
if len(df_debug_active) > 0:
    log.info(f"Date range: {df_debug_active['nav_date'].min().date()} to {df_debug_active['nav_date'].max().date()}")

debug_data['7_before_reindex'] = df_debug_active.copy()

# Perform reindex
all_dates = pd.date_range(df_active["nav_date"].min(), today, freq="D")
idx = pd.MultiIndex.from_product([active_codes, all_dates], names=["scheme_code", "nav_date"])

df_filled = (
    df_active.set_index(["scheme_code", "nav_date"])
             .reindex(idx)
             .groupby(level=0)
             .ffill()
             .reset_index()
)

df_debug_filled = df_filled[df_filled["scheme_code"] == DEBUG_SCHEME_CODE].copy()
log.info(f"Records after reindex & forward fill: {len(df_debug_filled)}")

debug_data['7_after_reindex'] = df_debug_filled.copy()

# DEBUG STEP 8: Return Calculation
log.info("=" * 80)
log.info("STEP 8: RETURN CALCULATION")
log.info("=" * 80)

nav_pivot = df_filled.pivot_table(index="nav_date", columns="scheme_code", values="nav")

if DEBUG_SCHEME_CODE in nav_pivot.columns:
    log.info(f"Scheme {DEBUG_SCHEME_CODE} found in pivot table")
    log.info(f"NAV entries: {nav_pivot[DEBUG_SCHEME_CODE].notna().sum()} / {len(nav_pivot)}")
else:
    log.warning(f"Scheme {DEBUG_SCHEME_CODE} NOT FOUND in pivot table")

def get_nav_at_offset(pivot: pd.DataFrame, anchor: datetime, days: int) -> pd.Series:
    target = anchor - timedelta(days=days)
    available = pivot.index[pivot.index <= target]
    if available.empty: 
        return pd.Series(dtype=float, name=f"nav_{days}d")
    result = pivot.loc[available[-1]].copy()
    result.name = f"nav_{days}d"
    return result

# Calculate returns for debug scheme
returns_debug = {"scheme_code": DEBUG_SCHEME_CODE}

for d in config["return_periods_days"]:
    past_nav_series = get_nav_at_offset(nav_pivot, today, d)
    
    if DEBUG_SCHEME_CODE in past_nav_series.index:
        past_nav = past_nav_series[DEBUG_SCHEME_CODE]
        
        scheme_audit = audit_trail[audit_trail["scheme_code"] == DEBUG_SCHEME_CODE]
        if len(scheme_audit) > 0:
            current_nav = scheme_audit.iloc[0]["latest_nav"]
            
            if pd.notna(past_nav) and past_nav > 0:
                return_pct = ((current_nav - past_nav) / past_nav * 100)
                log.info(f"{d}d return: Current NAV={current_nav:.4f}, Past NAV={past_nav:.4f}, Return={return_pct:.2f}%")
                returns_debug[f"nav_{d}d"] = past_nav
                returns_debug[f"return_{d}d"] = round(return_pct, 2)
            else:
                log.warning(f"{d}d return: Past NAV not available or invalid")
                returns_debug[f"nav_{d}d"] = None
                returns_debug[f"return_{d}d"] = None
    else:
        log.warning(f"{d}d return: Scheme not found in past NAV series")
        returns_debug[f"nav_{d}d"] = None
        returns_debug[f"return_{d}d"] = None

debug_data['8_returns'] = pd.DataFrame([returns_debug])

# Full return calculation for audit trail
for d in config["return_periods_days"]:
    past_nav = get_nav_at_offset(nav_pivot, today, d)
    audit_trail = audit_trail.merge(
        past_nav.reset_index().rename(columns={"scheme_code": "scheme_code", past_nav.name: f"nav_{d}d"}),
        on="scheme_code", how="left"
    )
    mask = (audit_trail["status"] == "Active") & (audit_trail[f"nav_{d}d"] > 0)
    audit_trail.loc[mask, f"return_{d}d"] = (
        (audit_trail.loc[mask, "latest_nav"] - audit_trail.loc[mask, f"nav_{d}d"]) 
        / audit_trail.loc[mask, f"nav_{d}d"] * 100
    ).round(2)

# Get final audit entry for debug scheme
debug_audit = audit_trail[audit_trail["scheme_code"] == DEBUG_SCHEME_CODE].copy()
debug_data['8_final_audit'] = debug_audit.copy()

# DEBUG STEP 9: Metadata
log.info("=" * 80)
log.info("STEP 9: METADATA ENRICHMENT")
log.info("=" * 80)

_CAT_PATTERN = re.compile(r'^(.*?)\s*\(\s*(.*?)\s*\)$')
_PLAN_PATTERN = re.compile(r'\b(Direct|Regular)\b', re.IGNORECASE)
_OPTION_PATTERN = re.compile(r'\b(IDCW|Dividend|Bonus|Growth)\b', re.IGNORECASE)

def split_category(cat_str):
    if not isinstance(cat_str, str): return pd.Series(["NA", "NA", "NA"])
    m = _CAT_PATTERN.match(cat_str.strip())
    if not m: return pd.Series([cat_str.strip(), "NA", "NA"])
    main = m.group(1).strip()
    parts = [p.strip() for p in m.group(2).split(" - ")]
    return pd.Series([main, parts[0] if len(parts)>0 else "NA", parts[1] if len(parts)>1 else "NA"])

meta_raw[["cat_level_1", "cat_level_2", "cat_level_3"]] = meta_raw["scheme_category"].apply(split_category)
meta_raw["plan_type"] = meta_raw["scheme_name"].apply(lambda x: (m := _PLAN_PATTERN.search(str(x))) and m.group(1).capitalize() or "Regular")
meta_raw["option_type"] = meta_raw["scheme_name"].apply(lambda x: (m := _OPTION_PATTERN.search(str(x))) and (m.group(1).upper().replace("DIVIDEND", "IDCW").capitalize()) or "Growth")

debug_meta = meta_raw[meta_raw["scheme_code"] == DEBUG_SCHEME_CODE].copy()
if len(debug_meta) > 0:
    log.info(f"Metadata found for {DEBUG_SCHEME_CODE}:")
    log.info(f"  Scheme Name: {debug_meta.iloc[0]['scheme_name']}")
    log.info(f"  AMC: {debug_meta.iloc[0]['amc_name']}")
    log.info(f"  Category: {debug_meta.iloc[0]['scheme_category']}")
    log.info(f"  Plan Type: {debug_meta.iloc[0]['plan_type']}")
    log.info(f"  Option Type: {debug_meta.iloc[0]['option_type']}")
else:
    log.warning(f"No metadata found for {DEBUG_SCHEME_CODE}")

debug_data['9_metadata'] = debug_meta.copy()

# ==========================
# EXPORT DEBUG DATA
# ==========================
log.info("=" * 80)
log.info("EXPORTING DEBUG DATA")
log.info("=" * 80)

os.makedirs("output", exist_ok=True)

with pd.ExcelWriter(f"output/debug_scheme_{DEBUG_SCHEME_CODE}.xlsx", engine="xlsxwriter") as writer:
    # Summary sheet
    summary_data = {
        "Metric": [
            "Daily DB Records",
            "Historic DB Records",
            "After UNION",
            "After Date Parse",
            "After Deduplication",
            "After Active Filter",
            "After Reindex",
            "Latest NAV Date",
            "Latest NAV Value",
            "Status",
            "Days Since Update"
        ],
        "Value": [
            len(debug_data.get('1_daily_raw', [])),
            len(debug_data.get('1_historic_raw', [])),
            len(debug_data.get('2_union_raw', [])),
            len(debug_data.get('3_after_date_parse', [])),
            len(debug_data.get('4_after_dedup', [])),
            len(debug_data.get('7_before_reindex', [])),
            len(debug_data.get('7_after_reindex', [])),
            scheme_latest.date() if pd.notna(scheme_latest) else "N/A",
            scheme_latest_record.iloc[0]["nav"] if len(scheme_latest_record) > 0 else "N/A",
            status if len(scheme_latest_record) > 0 else "N/A",
            (today - latest_date).days if len(scheme_latest_record) > 0 else "N/A"
        ]
    }
    pd.DataFrame(summary_data).to_excel(writer, sheet_name="Summary", index=False)
    
    # All intermediate data
    for key, data in debug_data.items():
        if isinstance(data, pd.DataFrame) and len(data) > 0:
            sheet_name = key[:31]  # Excel sheet name limit
            data.to_excel(writer, sheet_name=sheet_name, index=False)
    
    # Complete timeline with returns
    if len(df_debug_filled) > 0:
        timeline = df_debug_filled.sort_values('nav_date').copy()
        timeline['date'] = timeline['nav_date'].dt.date
        
        # Add return calculations for each date
        for period in config["return_periods_days"]:
            timeline[f'nav_{period}d_ago'] = timeline['nav'].shift(period)
            timeline[f'return_{period}d'] = (
                (timeline['nav'] - timeline[f'nav_{period}d_ago']) / timeline[f'nav_{period}d_ago'] * 100
            ).round(2)
        
        timeline.to_excel(writer, sheet_name="Complete_Timeline", index=False)
    
    log.info(f"Debug data exported to output/debug_scheme_{DEBUG_SCHEME_CODE}.xlsx")

log.info("=" * 80)
log.info("DEBUG COMPLETE")
log.info("=" * 80)
log.info(f"Check output/debug_scheme_{DEBUG_SCHEME_CODE}.xlsx for detailed analysis")
