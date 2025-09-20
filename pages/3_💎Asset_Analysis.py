import requests
import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime, timedelta

# ===================
# Axelar Tokens Dashboard (robust timestamp handling)
# ===================
API_BASE = "https://api.axelarscan.io"

@st.cache_data
def get_its_assets():
    r = requests.get(f"{API_BASE}/api/getITSAssets")
    r.raise_for_status()
    return r.json()

@st.cache_data
def get_gateway_assets():
    r = requests.get(f"{API_BASE}/api/getAssets")
    r.raise_for_status()
    return r.json()

@st.cache_data
def get_chart_data(endpoint, asset, from_time, to_time):
    url = f"{API_BASE}{endpoint}?asset={asset}&fromTime={from_time}&toTime={to_time}"
    resp = requests.get(url)
    if resp.status_code != 200:
        return pd.DataFrame()
    try:
        j = resp.json()
    except Exception:
        return pd.DataFrame()
    if isinstance(j, dict) and "data" in j:
        data = j["data"]
    elif isinstance(j, list):
        data = j
    else:
        return pd.DataFrame()
    return pd.DataFrame(data)

# Robust timestamp converter
def convert_timestamp_series(s):
    s_num = pd.to_numeric(s, errors='coerce')
    if s_num.dropna().empty:
        return pd.to_datetime(s, errors='coerce', utc=True)
    maxv = s_num.max()
    if maxv > 1e17:
        unit = 'ns'
    elif maxv > 1e14:
        unit = 'us'
    elif maxv > 1e11:
        unit = 'ms'
    else:
        unit = 's'
    try:
        ts = pd.to_datetime(s_num, unit=unit, errors='coerce', utc=True)
    except Exception:
        ts = pd.to_datetime(s, errors='coerce', utc=True)
    return ts

# Streamlit UI
st.set_page_config(page_title="Axelar Token Dashboard", layout="wide")
st.title("📊 Axelar Token Transfers & GMP Dashboard")

# Time filters
col1, col2, col3 = st.columns(3)
with col1:
    start_date = st.date_input("Start Date", datetime.utcnow() - timedelta(days=30))
with col2:
    end_date = st.date_input("End Date", datetime.utcnow())
with col3:
    timeframe = st.selectbox("Timeframe (aggregation)", ["day", "week", "month"])

freq_map = {"day": "D", "week": "W", "month": "M"}
freq = freq_map.get(timeframe, "D")

from_time = int(datetime.combine(start_date, datetime.min.time()).timestamp())
to_time = int(datetime.combine(end_date, datetime.min.time()).timestamp())

# Load assets
gateway_assets = get_gateway_assets()
its_assets = get_its_assets()

# Build combined table
gateway_df = pd.DataFrame(gateway_assets)
if not gateway_df.empty:
    gateway_df["type"] = "Gateway"
else:
    gateway_df = pd.DataFrame(columns=["id","denom","native_chain","name","symbol","decimals","image","coingecko_id","addresses","type"])

its_df = pd.DataFrame(its_assets)
if not its_df.empty:
    its_df["type"] = "ITS"
else:
    its_df = pd.DataFrame(columns=["id","symbol","decimals","image","coingecko_id","addresses","type"])

all_tokens = pd.concat([gateway_df, its_df], ignore_index=True, sort=False)
st.subheader("📋 Token List")
st.dataframe(all_tokens)

# ===================
# Fetch and process transfers/GMP data
# ===================
results = []
progress = st.progress(0)
count = 0
total = len(gateway_df) + len(its_df)

def sanitize_chart_df(df):
    if df is None or df.empty:
        return pd.DataFrame()
    for c in ["num_txs", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        else:
            df[c] = 0
    if "timestamp" not in df.columns:
        return pd.DataFrame()
    df["timestamp"] = convert_timestamp_series(df["timestamp"])
    df = df.dropna(subset=["timestamp"])
    return df[["timestamp", "num_txs", "volume"]]

for _, token in gateway_df.iterrows():
    denom = token.get("denom")
    symbol = token.get("symbol") or denom
    count += 1
    raw = get_chart_data("/token/transfersChart", denom, from_time, to_time)
    df = sanitize_chart_df(raw)
    if not df.empty:
        df["token"] = symbol
        df["type"] = "Gateway"
        results.append(df)
    progress.progress(int(count/total*100))

for _, token in its_df.iterrows():
    symbol = token.get("symbol")
    count += 1
    raw = get_chart_data("/gmp/GMPChart", symbol, from_time, to_time)
    df = sanitize_chart_df(raw)
    if not df.empty:
        df["token"] = symbol
        df["type"] = "ITS"
        results.append(df)
    progress.progress(int(count/total*100))

if not results:
    st.warning("No time-series data available for the selected tokens / period.")
    st.stop()

full_df = pd.concat(results, ignore_index=True, sort=False)
full_df = full_df.set_index("timestamp")

try:
    grouped = (
        full_df
        .groupby(["token", "type"]).resample(freq)[["num_txs", "volume"]]
        .sum()
        .reset_index()
    )
except Exception as e:
    st.error(f"Resample error: {e}")
    st.stop()

# ===================
# Charts (Plotly)
# ===================
st.subheader("📊 Transfers Over Time (Clustered Bar)")
fig = px.bar(grouped, x="timestamp", y="num_txs", color="token", barmode="group")
st.plotly_chart(fig, use_container_width=True)

st.subheader("📊 Volume Over Time (Clustered Bar)")
fig = px.bar(grouped, x="timestamp", y="volume", color="token", barmode="group")
st.plotly_chart(fig, use_container_width=True)

totals = grouped.groupby("token")[ ["num_txs","volume"] ].sum().sort_values("volume", ascending=False).reset_index()

st.subheader("📊 Total Transfers by Token (Horizontal Bar)")
fig = px.bar(totals, x="num_txs", y="token", orientation='h')
st.plotly_chart(fig, use_container_width=True)

st.subheader("📊 Total Volume by Token (Horizontal Bar)")
fig = px.bar(totals, x="volume", y="token", orientation='h')
st.plotly_chart(fig, use_container_width=True)

agg_type = grouped.groupby("type")[ ["num_txs","volume"] ].sum().reset_index()

st.subheader("📊 Transfers by ITS vs Gateway (Clustered Bar)")
fig = px.bar(agg_type, x="type", y="num_txs", color="type", barmode="group")
st.plotly_chart(fig, use_container_width=True)

st.subheader("📊 Volume by ITS vs Gateway (Clustered Bar)")
fig = px.bar(agg_type, x="type", y="volume", color="type", barmode="group")
st.plotly_chart(fig, use_container_width=True)

st.subheader("🥧 Share of Transfers (ITS vs Gateway)")
fig = px.pie(agg_type, names="type", values="num_txs", hole=0.3)
st.plotly_chart(fig, use_container_width=True)

st.subheader("🥧 Share of Volume (ITS vs Gateway)")
fig = px.pie(agg_type, names="type", values="volume", hole=0.3)
st.plotly_chart(fig, use_container_width=True)

st.subheader("🔎 Top tokens by volume")
st.dataframe(totals.head(20))

st.success("Dashboard ready ✅")
