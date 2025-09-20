import requests
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# ===================
# Helper Functions
# ===================
API_BASE = "https://api.axelarscan.io"

@st.cache_data
def get_its_assets():
    return requests.get(f"{API_BASE}/api/getITSAssets").json()

@st.cache_data
def get_gateway_assets():
    return requests.get(f"{API_BASE}/api/getAssets").json()

@st.cache_data
def get_chart_data(endpoint, asset, from_time, to_time):
    url = f"{API_BASE}{endpoint}?asset={asset}&fromTime={from_time}&toTime={to_time}"
    resp = requests.get(url).json()
    return pd.DataFrame(resp["data"])

# ===================
# Streamlit UI
# ===================
st.set_page_config(page_title="Axelar Token Dashboard", layout="wide")
st.title("📊 Axelar Token Transfers & GMP Dashboard")

# Time filters
col1, col2, col3 = st.columns(3)
with col1:
    start_date = st.date_input("Start Date", datetime.utcnow() - timedelta(days=30))
with col2:
    end_date = st.date_input("End Date", datetime.utcnow())
with col3:
    timeframe = st.selectbox("Timeframe", ["day", "week", "month"])

from_time = int(datetime.combine(start_date, datetime.min.time()).timestamp())
to_time = int(datetime.combine(end_date, datetime.min.time()).timestamp())

# Load assets
gateway_assets = get_gateway_assets()
its_assets = get_its_assets()

# Build combined table
gateway_df = pd.DataFrame(gateway_assets)
gateway_df["type"] = "Gateway"

its_df = pd.DataFrame(its_assets)
its_df["type"] = "ITS"

all_tokens = pd.concat([gateway_df, its_df], ignore_index=True)
st.subheader("📋 Token List")
st.dataframe(all_tokens)

# ===================
# Fetch and process volume & tx data
# ===================
results = []

for _, token in gateway_df.iterrows():
    denom = token["denom"]
    df = get_chart_data("/token/transfersChart", denom, from_time, to_time)
    if not df.empty:
        df["token"] = token["symbol"]
        df["type"] = "Gateway"
        results.append(df)

for _, token in its_df.iterrows():
    symbol = token["symbol"]
    df = get_chart_data("/gmp/GMPChart", symbol, from_time, to_time)
    if not df.empty:
        df["token"] = token["symbol"]
        df["type"] = "ITS"
        results.append(df)

if not results:
    st.warning("No data available for selected period.")
    st.stop()

full_df = pd.concat(results, ignore_index=True)
full_df["timestamp"] = pd.to_datetime(full_df["timestamp"], unit="s")

# Resample data
full_df = full_df.set_index("timestamp")
full_df = full_df.groupby(["token", "type"]).resample(timeframe)[["num_txs", "volume"]].sum().reset_index()

# ===================
# Charts
# ===================

st.subheader("📊 Transfers & Volume Over Time (Clustered Bar)")
fig, ax = plt.subplots(figsize=(12,6))
for key, grp in full_df.groupby("token"):
    ax.bar(grp["timestamp"], grp["num_txs"], label=key)
plt.legend()
st.pyplot(fig)

# Horizontal bar for totals
totals = full_df.groupby("token").sum().sort_values("volume", ascending=False)

st.subheader("📊 Total Transfers by Token (Horizontal Bar)")
fig, ax = plt.subplots(figsize=(10,6))
totals["num_txs"].plot(kind="barh", ax=ax)
st.pyplot(fig)

st.subheader("📊 Total Volume by Token (Horizontal Bar)")
fig, ax = plt.subplots(figsize=(10,6))
totals["volume"].plot(kind="barh", ax=ax)
st.pyplot(fig)

# ITS vs Gateway comparison
agg_type = full_df.groupby("type").sum()

st.subheader("📊 Transfers by ITS vs Gateway (Clustered Bar)")
fig, ax = plt.subplots()
agg_type[["num_txs"]].plot(kind="bar", ax=ax)
st.pyplot(fig)

st.subheader("📊 Volume by ITS vs Gateway (Clustered Bar)")
fig, ax = plt.subplots()
agg_type[["volume"]].plot(kind="bar", ax=ax)
st.pyplot(fig)

# Pie charts
st.subheader("🥧 Share of Transfers (ITS vs Gateway)")
fig, ax = plt.subplots()
agg_type["num_txs"].plot(kind="pie", autopct="%1.1f%%", ax=ax)
st.pyplot(fig)

st.subheader("🥧 Share of Volume (ITS vs Gateway)")
fig, ax = plt.subplots()
agg_type["volume"].plot(kind="pie", autopct="%1.1f%%", ax=ax)
st.pyplot(fig)

st.success("Dashboard ready ✅")

