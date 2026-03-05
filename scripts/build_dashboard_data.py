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
# 1. LOAD CONFIG & DOWNLOADS
# ==========================
with open("config.json", "r") as f:
    config = json.load(f)

# Derive anchor date from config (falls back to today if not set)
anchor_date_str = config.get("anchor_date")
if anchor_date_str:
    today = datetime.strptime(anchor_date_str, "%Y-%m-%d").replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    log.info("Using anchor date from config: %s", today.date())
else:
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    log.info("No anchor_date in config — using today: %s", today.date())

# Derive reindex window dynamically from config — never silently truncate data
max_period    = max(config["return_periods_days"])
buffer_days   = config.get("reindex_buffer_days", 15)
reindex_start = today - timedelta(days=max_period + buffer_days)

# Freshness threshold from config — no hardcoded magic numbers
freshness_days      = config.get("freshness_threshold_days", 5)
freshness_threshold = today - timedelta(days=freshness_days)

# ── Historic DB (cached) ──
if not os.path.exists("historic.db"):
    log.info("Cache miss: Downloading historic database from Google Drive...")
    gdown.download(config["historic_db_url"], "historic.db", quiet=True, fuzzy=True)
else:
    log.info("Cache hit: Using cached historic.db")

# ── Daily mf.db from GitHub Releases ──
log.info("Fetching daily mf.db from GitHub API...")
try:
    response     = requests.get(config["mf_release_api"], timeout=30)
    release_info = response.json()

    if "assets" not in release_info:
        raise RuntimeError(
            f"'assets' key missing in GitHub API response. "
            f"Check the repo/token. Response keys: {list(release_info.keys())}"
        )

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
# 2. DATA LOADING & CLEANING
# ==========================

# ── Vectorized date parser — >95% matching format wins ──
def parse_dates_vectorized(series: pd.Series) -> pd.Series:
    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%Y/%m/%d'):
        parsed = pd.to_datetime(series, format=fmt, errors='coerce')
        if parsed.notna().mean() > 0.95:
            return parsed
    # Last resort: let pandas infer
    return pd.to_datetime(series, errors='coerce')

with sqlite3.connect(":memory:") as conn:
    conn.execute("ATTACH DATABASE 'mf.db' AS daily;")
    conn.execute("ATTACH DATABASE 'historic.db' AS historic;")

    # UNION ALL (not UNION) to preserve every row — deduplicate explicitly below
    # so daily data takes priority over historic on any date conflict
    query = """
        SELECT scheme_code, nav, nav_date, 'daily'    AS source FROM daily.nav_history
        UNION ALL
        SELECT scheme_code, nav_value AS nav, nav_date, 'historic' AS source FROM historic.nav_history
    """
    df = pd.read_sql_query(query, conn)

    # Metadata — pulled inside the same connection context
    meta_query = """
        SELECT DISTINCT scheme_code, scheme_name, amc_name, scheme_category
        FROM daily.nav_history
    """
    meta_raw = pd.read_sql_query(meta_query, conn)

log.info("Loaded %d raw NAV rows across %d schemes.", len(df), df["scheme_code"].nunique())

# ── Date parsing & deduplication ──
df["nav_date"] = parse_dates_vectorized(df["nav_date"])
df = df.dropna(subset=["nav_date"])
df = df[df["nav_date"] <= today]

# Daily rows win on (scheme_code, nav_date) conflicts
df = (
    df.sort_values("source")                                    # 'daily' sorts before 'historic'
      .drop_duplicates(["scheme_code", "nav_date"], keep="first")
      .drop(columns=["source"])
)

log.info("After dedup: %d rows, %d schemes.", len(df), df["scheme_code"].nunique())

# ==========================
# 3. IDENTIFY STATUS (STALE VS ACTIVE)
# ==========================
latest_nav_df = df.sort_values("nav_date").groupby("scheme_code").tail(1).copy()

audit_trail = latest_nav_df[["scheme_code", "nav_date", "nav"]].rename(
    columns={"nav_date": "latest_nav_date", "nav": "latest_nav"}
)
audit_trail["status"] = audit_trail["latest_nav_date"].apply(
    lambda x: "Active" if x >= freshness_threshold else "Excluded: Stale Data"
)

active_count  = (audit_trail["status"] == "Active").sum()
stale_count   = (audit_trail["status"] != "Active").sum()
log.info("Scheme status — Active: %d | Stale/Excluded: %d", active_count, stale_count)

# ==========================
# 4. FAST VECTORIZED REINDEX (ACTIVE ONLY)
# ==========================
active_codes = audit_trail[audit_trail["status"] == "Active"]["scheme_code"].unique()
df_active    = df[df["scheme_code"].isin(active_codes)].copy()

# Only reindex the window we actually need — derived from config, never hardcoded
df_active = df_active[df_active["nav_date"] >= reindex_start].sort_values(
    ["scheme_code", "nav_date"]
)
log.info(
    "Reindexing %d active schemes from %s to %s...",
    len(active_codes), reindex_start.date(), today.date()
)

all_dates = pd.date_range(df_active["nav_date"].min(), today, freq="D")
idx = pd.MultiIndex.from_product(
    [active_codes, all_dates], names=["scheme_code", "nav_date"]
)

df_filled = (
    df_active.set_index(["scheme_code", "nav_date"])
             .reindex(idx)
             .groupby(level=0)
             .ffill()
             .reset_index()
)

# ==========================
# 5. COMPUTE RETURNS (PIVOT — ONE PASS)
# ==========================
log.info("Computing returns via pivot table (single pass)...")

# Pivot once: rows = dates, columns = scheme_code → O(1) per period lookup
nav_pivot = df_filled.pivot_table(
    index="nav_date", columns="scheme_code", values="nav"
)

def get_nav_at_offset(pivot: pd.DataFrame, anchor: datetime, days: int) -> pd.Series:
    """Return the last available NAV on or before (anchor - days)."""
    target    = anchor - timedelta(days=days)
    available = pivot.index[pivot.index <= target]
    if available.empty:
        log.warning("No NAV data available for %d-day lookback (target: %s).", days, target.date())
        return pd.Series(dtype=float, name=f"nav_{days}d")
    result = pivot.loc[available[-1]].copy()
    result.name = f"nav_{days}d"
    return result

for d in config["return_periods_days"]:
    past_nav = get_nav_at_offset(nav_pivot, today, d)
    audit_trail = audit_trail.merge(
        past_nav.reset_index().rename(columns={"scheme_code": "scheme_code", past_nav.name: f"nav_{d}d"}),
        on="scheme_code",
        how="left"
    )
    mask = (
        (audit_trail["status"] == "Active") &
        audit_trail[f"nav_{d}d"].notna() &
        (audit_trail[f"nav_{d}d"] > 0)
    )
    audit_trail.loc[mask, f"return_{d}d"] = (
        (audit_trail.loc[mask, "latest_nav"] - audit_trail.loc[mask, f"nav_{d}d"])
        / audit_trail.loc[mask, f"nav_{d}d"]
        * 100
    ).round(2)

log.info("Returns computed for periods: %s", config["return_periods_days"])

# ==========================
# 6. METADATA PROCESSING
# ==========================

# Regex compiled once — not per row
_CAT_PATTERN  = re.compile(r'^(.*?)\s*\(\s*(.*?)\s*\)$')
_PLAN_PATTERN = re.compile(r'\b(Direct|Regular)\b', re.IGNORECASE)

def split_category(cat_str) -> pd.Series:
    if not cat_str or not isinstance(cat_str, str):
        return pd.Series(["NA", "NA", "NA"])
    m = _CAT_PATTERN.match(cat_str.strip())
    if not m:
        return pd.Series([cat_str.strip(), "NA", "NA"])
    main  = m.group(1).strip()
    parts = [p.strip() for p in m.group(2).split(" - ")]
    return pd.Series([
        main,
        parts[0] if len(parts) > 0 else "NA",
        parts[1] if len(parts) > 1 else "NA",
    ])

def detect_plan_type(name: str) -> str:
    """Check for ' - Direct' / ' - Regular' as a word boundary, not bare substring match."""
    m = _PLAN_PATTERN.search(str(name))
    return m.group(1).capitalize() if m else "Regular"

meta_raw[["cat_level_1", "cat_level_2", "cat_level_3"]] = meta_raw["scheme_category"].apply(split_category)
meta_raw["plan_type"] = meta_raw["scheme_name"].apply(detect_plan_type)

# ==========================
# 7. FINAL EXPORT
# ==========================
analytics_dashboard = (
    audit_trail[audit_trail["status"] == "Active"]
    .merge(meta_raw, on="scheme_code", how="left")
)

os.makedirs("output", exist_ok=True)
output_path = "output/dashboard_data.xlsx"

with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
    analytics_dashboard.to_excel(writer, sheet_name="Active_Analytics", index=False)
    audit_trail.to_excel(writer, sheet_name="Full_Audit_Trail", index=False)

log.info(
    "Dashboard ready → %s | Active schemes: %d | Anchor date: %s",
    output_path, len(analytics_dashboard), today.date()
)
