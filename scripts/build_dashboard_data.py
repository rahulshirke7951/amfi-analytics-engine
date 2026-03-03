import sqlite3
import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta

# ==========================
# LOAD CONFIG
# ==========================

with open("config.json", "r") as f:
    config = json.load(f)

HISTORIC_DB_URL = config["historic_db_url"]
MF_RELEASE_API = config["mf_release_api"]
ANCHOR_DATE = config["anchor_date"]
RETURN_PERIODS = config["return_periods_days"]

OUTPUT_FILE = "output/dashboard_data.xlsx"
os.makedirs("output", exist_ok=True)
