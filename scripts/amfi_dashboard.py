import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime

# ─────────────────────────────────────────────
# 1. PAGE CONFIG & THEME
# ─────────────────────────────────────────────
st.set_page_config(page_title="AMFI Executive Analytics", layout="wide")

# ─────────────────────────────────────────────
# 2. DATA LOADING (BACKGROUND AUDIT)
# ─────────────────────────────────────────────
@st.cache_data
def load_data():
    file_path = "output/dashboard_data.xlsx"
    if os.path.exists(file_path):
        # We load the main dashboard data
        # The 'Audit_Trail' sheet remains in the Excel file for background checks
        return pd.read_excel(file_path, sheet_name="Analytics_Dashboard")
    return None

df = load_data()

# ─────────────────────────────────────────────
# 3. DASHBOARD UI
# ─────────────────────────────────────────────
if df is not None:
    st.title("💎 AMFI Executive Performance")
    
    # KPI Ribbon
    c1, c2, c3 = st.columns(3)
    with c1: st.metric("Total Funds", f"{len(df):,}")
    with c2: st.metric("Market Avg (30D)", f"{df['return_30d'].mean():.2f}%")
    with c3: st.metric("Last Sync", pd.to_datetime(df['latest_nav_date']).max().strftime('%d %b'))

    # THE FIX: Treemap with Unique Leaves
    st.subheader("Asset Allocation & Performance Map")
    fig = px.treemap(
        df, 
        # Added scheme_name to ensure every leaf is unique and avoid ValueError
        path=['cat_level_1', 'cat_level_2', 'cat_level_3', 'scheme_name'], 
        values='latest_nav',
        color='return_30d',
        color_continuous_scale='RdYlGn',
        template="plotly_dark",
        hover_data=['scheme_name', 'return_30d']
    )
    fig.update_layout(margin=dict(t=30, l=10, r=10, b=10), height=600)
    st.plotly_chart(fig, use_container_width=True)

    # Presentation Filters
    st.divider()
    col_a, col_b = st.columns([1, 2])
    with col_a:
        st.write("### Filter Analysis")
        cat1 = st.selectbox("Category Level 1", df['cat_level_1'].unique())
        plan = st.radio("Plan Type", ["Direct Plan", "Regular Plan"], horizontal=True)
        
    with col_b:
        filtered = df[(df['cat_level_1'] == cat1) & (df['plan_type'] == plan)]
        st.write(f"### Top Performers: {cat1}")
        st.dataframe(
            filtered.nlargest(10, 'return_30d')[['scheme_name', 'return_30d', 'latest_nav']],
            use_container_width=True
        )
else:
    st.error("⚠️ dashboard_data.xlsx not found. Please run the GitHub Action.")
