import streamlit as st
import pandas as pd
import os

# ─────────────────────────────────────────────
# DATA LOADING (Optimized for your Excel File)
# ─────────────────────────────────────────────
@st.cache_data
def load_excel_data():
    file_path = "output/dashboard_data.xlsx"
    if os.path.exists(file_path):
        # Read both sheets
        analytics_df = pd.read_excel(file_path, sheet_name="Analytics_Dashboard")
        audit_df = pd.read_excel(file_path, sheet_name="Audit_Trail")
        return analytics_df, audit_df
    return None, None

df, audit_df = load_excel_data()

if df is not None:
    # 1. KPI Section
    latest_update = pd.to_datetime(df['latest_nav_date']).max().strftime('%d-%b-%Y')
    st.metric("System Refresh Date", latest_update)

    # 2. Main Dashboard (From Claude Example)
    st.subheader("Fund Performance Overview")
    st.dataframe(df, use_container_width=True)

    # 3. Validation Section (The "Audit Trail")
    with st.expander("🔍 View Audit Trail (Data Verification)"):
        st.write("This table shows the raw values used for return calculations.")
        st.dataframe(audit_df)
else:
    st.error("Dashboard data not found. Please run the GitHub Action.")
