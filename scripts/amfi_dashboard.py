import streamlit as st
import pandas as pd
import plotly.express as px
import os

# ─────────────────────────────────────────────
# DATA & SYSTEM SETTINGS
# ─────────────────────────────────────────────
@st.cache_data
def load_presentation_data():
    file_path = "output/dashboard_data.xlsx"
    if os.path.exists(file_path):
        # We load the Audit sheet but don't display it to keep the UI clean
        df = pd.read_excel(file_path, sheet_name="Analytics_Dashboard")
        return df
    return None

df = load_presentation_data()

# ─────────────────────────────────────────────
# DASHBOARD LAYOUT
# ─────────────────────────────────────────────
if df is not None:
    st.title("💎 AMFI Executive Analytics")
    
    # 1. THE KPI RIBBON
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("Market Coverage", f"{df['amc_name'].nunique()} AMCs")
    with c2: st.metric("Avg. 30D Growth", f"{df['return_30d'].mean():.2f}%")
    with c3: st.metric("Top Category", df['cat_level_2'].value_counts().idxmax())
    with c4: st.metric("Last Data Sync", pd.to_datetime(df['latest_nav_date']).max().strftime('%d %b'))

    # 2. SECTOR HEATMAP (Visualizing Cat Levels)
    st.subheader("Performance Map by Asset Class")
    fig = px.treemap(df, 
                     path=['cat_level_1', 'cat_level_2', 'cat_level_3'], 
                     values='latest_nav',
                     color='return_30d',
                     color_continuous_scale='RdYlGn',
                     hover_data=['scheme_name'])
    fig.update_layout(margin=dict(t=30, l=10, r=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

    # 3. PRESENTATION FILTER (Side-by-Side Comparison)
    col_left, col_right = st.columns([1, 2])
    with col_left:
        st.write("### Filter Selection")
        selected_cat = st.selectbox("Asset Class (Cat 1)", df['cat_level_1'].unique())
        selected_plan = st.radio("Plan Type", ["Regular Plan", "Direct Plan"], horizontal=True)
    
    with col_right:
        filtered_df = df[(df['cat_level_1'] == selected_cat) & (df['plan_type'] == selected_plan)]
        st.write(f"### Top 10 Performers in {selected_cat}")
        st.table(filtered_df.nlargest(10, 'return_30d')[['scheme_name', 'return_30d', 'latest_nav']])

else:
    st.error("Please run the GitHub Action to generate 'dashboard_data.xlsx' first.")
