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
    # Some endpoints return {"data": [...]} according to your spec
    if isinstance(j, dict) and "data" in j:
        data = j["data"]
    elif isinstance(j, list):
        data = j
    else:
        return pd.DataFrame()
    return pd.DataFrame(data)

# Robust timestamp converter that auto-detects seconds/ms/us/ns
def convert_timestamp_series(s):
    # try numeric first
    s_num = pd.to_numeric(s, errors='coerce')
    if s_num.dropna().empty:
        # maybe already ISO strings
        return pd.to_datetime(s, errors='coerce', utc=True)

    maxv = s_num.max()
    # choose unit thresholds (empirical)
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
        # fallback to generic parsing
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

# map timeframe to pandas resample freq
freq_map = {"day": "D", "week": "W", "month": "M"}
freq = freq_map.get(timeframe, "D")

from_time = int(datetime.combine(start_date, datetime.min.time()).timestamp())
to_time = int(datetime.combine(end_date, datetime.min.time()).timestamp())

# Load assets
try:
    gateway_assets = get_gateway_assets()
    its_assets = get_its_assets()
except Exception as e:
    st.error(f"Failed to fetch assets: {e}")
    st.stop()

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
# Fetch and process transfers/GMP data for each token
# ===================
results = []
progress = st.progress(0)
count = 0
total = len(gateway_df) + len(its_df)

# Helper to sanitize dataframe from API
def sanitize_chart_df(df):
    if df is None or df.empty:
        return pd.DataFrame()
    # ensure numeric columns exist
    for c in ["num_txs", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        else:
            df[c] = 0
    if "timestamp" not in df.columns:
        return pd.DataFrame()
    df["timestamp_converted"] = convert_timestamp_series(df["timestamp"])
    # drop rows we couldn't parse
    df = df.dropna(subset=["timestamp_converted"]) 
    # use converted ts as canonical timestamp column
    df = df.rename(columns={"timestamp_converted": "timestamp"})
    df = df[["timestamp", "num_txs", "volume"]]
    return df

# Gateway tokens
for _, token in gateway_df.iterrows():
    denom = token.get("denom")
    symbol = token.get("symbol") or denom
    count += 1
    try:
        raw = get_chart_data("/token/transfersChart", denom, from_time, to_time)
        df = sanitize_chart_df(raw)
        if not df.empty:
            df["token"] = symbol
            df["type"] = "Gateway"
            results.append(df)
    except Exception as e:
        st.warning(f"Error fetching gateway token {denom}: {e}")
    progress.progress(int(count/total*100))

# ITS tokens (GMPChart using symbol as asset per your description)
for _, token in its_df.iterrows():
    symbol = token.get("symbol")
    count += 1
    try:
        raw = get_chart_data("/gmp/GMPChart", symbol, from_time, to_time)
        df = sanitize_chart_df(raw)
        if not df.empty:
            df["token"] = symbol
            df["type"] = "ITS"
            results.append(df)
    except Exception as e:
        st.warning(f"Error fetching ITS token {symbol}: {e}")
    progress.progress(int(count/total*100))

if not results:
    st.warning("No time-series data available for the selected tokens / period.")
    st.stop()

full_df = pd.concat(results, ignore_index=True, sort=False)
# ensure timestamp is datetime index
if not pd.api.types.is_datetime64_any_dtype(full_df["timestamp"]):
    full_df["timestamp"] = pd.to_datetime(full_df["timestamp"], errors='coerce')

full_df = full_df.dropna(subset=["timestamp"])  # safety
full_df = full_df.set_index("timestamp")

# Resample per token+type
try:
    grouped = (
        full_df
        .groupby(["token", "type"])    # group by token & type
        .resample(freq)[["num_txs", "volume"]]
        .sum()
        .reset_index()
    )
except Exception as e:
    st.error(f"Failed to resample/aggregate time-series: {e}")
    st.stop()

# ===================
# Charts (Plotly)
# ===================
st.subheader("📊 Transfers Over Time (Clustered Bar)")
fig = px.bar(grouped, x="timestamp", y="num_txs", color="token", barmode="group", title="Number of Transfers over time")
st.plotly_chart(fig, use_container_width=True)

st.subheader("📊 Volume Over Time (Clustered Bar)")
fig = px.bar(grouped, x="timestamp", y="volume", color="token", barmode="group", title="Volume over time")
st.plotly_chart(fig, use_container_width=True)

# Totals per token
totals = grouped.groupby("token")[ ["num_txs","volume"] ].sum().sort_values("volume", ascending=False).reset_index()

st.subheader("📊 Total Transfers by Token (Horizontal Bar)")
fig = px.bar(totals, x="num_txs", y="token", orientation='h', title="Total transfers by token")
st.plotly_chart(fig, use_container_width=True)

st.subheader("📊 Total Volume by Token (Horizontal Bar)")
fig = px.bar(totals, x="volume", y="token", orientation='h', title="Total volume by token")
st.plotly_chart(fig, use_container_width=True)

# ITS vs Gateway
agg_type = grouped.groupby("type")[ ["num_txs","volume"] ].sum().reset_index()

st.subheader("📊 Transfers by ITS vs Gateway (Clustered Bar)")
fig = px.bar(agg_type, x="type", y="num_txs", color="type", barmode="group", title="Num transactions: ITS vs Gateway")
st.plotly_chart(fig, use_container_width=True)

st.subheader("📊 Volume by ITS vs Gateway (Clustered Bar)")
fig = px.bar(agg_type, x="type", y="volume", color="type", barmode="group", title="Volume: ITS vs Gateway")
st.plotly_chart(fig, use_container_width=True)

# Pie charts
st.subheader("🥧 Share of Transfers (ITS vs Gateway)")
fig = px.pie(agg_type, names="type", values="num_txs", hole=0.3)
st.plotly_chart(fig, use_container_width=True)

st.subheader("🥧 Share of Volume (ITS vs Gateway)")
fig = px.pie(agg_type, names="type", values="volume", hole=0.3)
st.plotly_chart(fig, use_container_width=True)

# Extra helpful tables/outputs
st.subheader("🔎 Top tokens by volume")
st.dataframe(totals.head(20))

st.success("Dashboard ready ✅")

# Notes for debugging
st.caption('If you still get timestamp-related errors: check a sample of raw timestamps by calling the APIs outside the dashboard and inspect the "timestamp" values (ms vs s).')
