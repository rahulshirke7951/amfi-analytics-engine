import streamlit as st
import pandas as pd
import plotly.express as px
import os

# ─────────────────────────────────────────────
# 1. PAGE CONFIG & STYLING
# ─────────────────────────────────────────────
st.set_page_config(page_title="AMFI Executive Performance", layout="wide")

# ─────────────────────────────────────────────
# 2. DATA LOADING
# ─────────────────────────────────────────────
@st.cache_data
def load_presentation_data():
    file_path = "output/dashboard_data.xlsx"
    if os.path.exists(file_path):
        # Loads main analytics; Audit Trail remains in background (Excel)
        return pd.read_excel(file_path, sheet_name="Analytics_Dashboard")
    return None

df = load_presentation_data()

# ─────────────────────────────────────────────
# 3. EXECUTIVE DASHBOARD UI
# ─────────────────────────────────────────────
if df is not None:
    st.title("💎 AMFI Executive Performance")
    
    # KPI Ribbon
    c1, c2, c3 = st.columns(3)
    with c1: st.metric("Total Funds", f"{len(df):,}")
    with c2: st.metric("Market Avg (30D)", f"{df['return_30d'].mean():.2f}%")
    with c3: st.metric("Last Sync", pd.to_datetime(df['latest_nav_date']).max().strftime('%d %b'))

    # 🚀 FIXED TREEMAP: Added scheme_name to the path to resolve ValueError
    st.subheader("Asset Allocation & Performance Map")
    fig = px.treemap(
        df, 
        path=['cat_level_1', 'cat_level_2', 'cat_level_3', 'scheme_name'], 
        values='latest_nav',
        color='return_30d',
        color_continuous_scale='RdYlGn',
        range_color=[-5, 5], # Adjust based on expected return range
        template="plotly_dark",
        hover_data=['scheme_name', 'return_30d']
    )
    fig.update_layout(margin=dict(t=30, l=10, r=10, b=10), height=650)
    st.plotly_chart(fig, use_container_width=True)

    # 4. DATA EXPLORER (Presentation Ready)
    st.divider()
    col_a, col_b = st.columns([1, 2])
    with col_a:
        st.write("### 🔍 Category Deep Dive")
        cat_filter = st.selectbox("Select Asset Class", df['cat_level_1'].unique())
        plan_filter = st.radio("Plan Type", ["Direct Plan", "Regular Plan"], horizontal=True)
        
    with col_b:
        filtered = df[(df['cat_level_1'] == cat_filter) & (df['plan_type'] == plan_filter)]
        st.write(f"### Top 10 Performers: {cat_filter}")
        st.dataframe(
            filtered.nlargest(10, 'return_30d')[['scheme_name', 'return_30d', 'latest_nav']],
            use_container_width=True,
            hide_index=True
        )
else:
    st.error("⚠️ Data file not found. Ensure GitHub Actions generated output/dashboard_data.xlsx")
