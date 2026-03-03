import streamlit as st
import pandas as pd
import plotly.express as px
import os

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="AMFI Analytics Engine",
    layout="wide",
)

st.title("📊 AMFI Analytics Engine")

# ─────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────
@st.cache_data
def load_data():
    file_path = "output/dashboard_data.xlsx"
    if os.path.exists(file_path):
        df = pd.read_excel(file_path, sheet_name="Analytics_Dashboard")
        return df
    return None

df = load_data()

if df is None:
    st.error("❌ dashboard_data.xlsx not found in output/ folder")
    st.stop()

# Convert date properly
df["latest_nav_date"] = pd.to_datetime(df["latest_nav_date"], errors="coerce")

# ─────────────────────────────────────────────
# FILTER SECTION
# ─────────────────────────────────────────────
st.divider()
st.subheader("🎛 Fund Filters")

col1, col2, col3, col4, col5 = st.columns(5)

# 1️⃣ Latest NAV Date (Display only)
with col1:
    latest_date = df["latest_nav_date"].max()
    st.metric("NAV Snapshot", latest_date.strftime("%d-%b-%Y"))

# 2️⃣ Category Level 1
with col2:
    lvl1_options = sorted(df["cat_level_1"].dropna().unique())
    default_lvl1 = "Open Ended Schemes"
    selected_lvl1 = st.selectbox(
        "Category Level 1",
        lvl1_options,
        index=lvl1_options.index(default_lvl1) if default_lvl1 in lvl1_options else 0
    )

# 3️⃣ Category Level 2 (dependent)
with col3:
    lvl2_options = sorted(
        df[df["cat_level_1"] == selected_lvl1]["cat_level_2"].dropna().unique()
    )
    default_lvl2 = "Equity Scheme"
    selected_lvl2 = st.selectbox(
        "Category Level 2",
        lvl2_options,
        index=lvl2_options.index(default_lvl2) if default_lvl2 in lvl2_options else 0
    )

# 4️⃣ Category Level 3 (dependent)
with col4:
    lvl3_options = sorted(
        df[
            (df["cat_level_1"] == selected_lvl1) &
            (df["cat_level_2"] == selected_lvl2)
        ]["cat_level_3"].dropna().unique()
    )
    default_lvl3 = "Sectoral/ Thematic"
    selected_lvl3 = st.selectbox(
        "Category Level 3",
        lvl3_options,
        index=lvl3_options.index(default_lvl3) if default_lvl3 in lvl3_options else 0
    )

# 5️⃣ Plan Type
with col5:
    plan_options = sorted(df["plan_type"].dropna().unique())
    default_plan = "Regular Plan"
    selected_plan = st.selectbox(
        "Plan Type",
        plan_options,
        index=plan_options.index(default_plan) if default_plan in plan_options else 0
    )

# ─────────────────────────────────────────────
# APPLY FILTERS
# ─────────────────────────────────────────────
filtered_df = df[
    (df["cat_level_1"] == selected_lvl1) &
    (df["cat_level_2"] == selected_lvl2) &
    (df["cat_level_3"] == selected_lvl3) &
    (df["plan_type"] == selected_plan)
]

# Optional: payout filter if column exists
if "payout_option" in df.columns:
    filtered_df = filtered_df[filtered_df["payout_option"] == "Growth"]

st.divider()

# ─────────────────────────────────────────────
# KPI SECTION (Safe Version)
# ─────────────────────────────────────────────
k1, k2, k3 = st.columns(3)

with k1:
    st.metric("Total Funds", len(filtered_df))

with k2:
    if (
        "return_30d" in filtered_df.columns and
        filtered_df["return_30d"].notna().any()
    ):
        avg_return = filtered_df["return_30d"].mean()
        st.metric("Avg 30D Return", f"{avg_return:.2f}%")
    else:
        st.metric("Avg 30D Return", "N/A")

with k3:
    if (
        "return_30d" in filtered_df.columns and
        filtered_df["return_30d"].notna().any()
    ):
        top_row = filtered_df.loc[
            filtered_df["return_30d"].idxmax()
        ]
        st.metric("Top Performer", top_row["scheme_name"])
    else:
        st.metric("Top Performer", "N/A")

# ─────────────────────────────────────────────
# TREEMAP
# ─────────────────────────────────────────────
st.subheader("📈 Performance Treemap")

if not filtered_df.empty and "return_30d" in filtered_df.columns:
    fig = px.treemap(
        filtered_df,
        path=["cat_level_2", "cat_level_3", "scheme_name"],
        values="latest_nav",
        color="return_30d",
        color_continuous_scale="RdYlGn",
        template="plotly_dark"
    )
    fig.update_layout(height=600)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No data available for selected filters.")

st.divider()

# ─────────────────────────────────────────────
# TOP 10 TABLE
# ─────────────────────────────────────────────
st.subheader("🏆 Top 10 Performers")

if not filtered_df.empty and "return_30d" in filtered_df.columns:
    top10 = filtered_df.nlargest(10, "return_30d")[
        ["scheme_name", "return_30d", "latest_nav"]
    ]
    st.dataframe(top10, use_container_width=True, hide_index=True)
else:
    st.info("No ranking data available.")
