import requests
import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime, timedelta

# =====================================================
# Page Config
# =====================================================
st.set_page_config(
    page_title="Axelar Master Dashboard",
    page_icon="https://axelarscan.io/logos/logo.png",
    layout="wide"
)

# =====================================================
# Sidebar Footer
# =====================================================
st.sidebar.markdown(
    """
    <style>
    .sidebar-footer {
        position: fixed;
        bottom: 20px;
        width: 250px;
        font-size: 13px;
        color: gray;
        margin-left: 5px; # -- MOVE LEFT
        text-align: left;  
    }
    .sidebar-footer img {
        width: 16px;
        height: 16px;
        vertical-align: middle;
        border-radius: 50%;
        margin-right: 5px;
    }
    .sidebar-footer a {
        color: gray;
        text-decoration: none;
    }
    </style>

    <div class="sidebar-footer">
        <div>
            <a href="https://x.com/axelar" target="_blank">
                <img src="https://img.cryptorank.io/coins/axelar1663924228506.png" alt="Axelar Logo">
                Powered by Axelar
            </a>
        </div>
        <div style="margin-top: 5px;">
            <a href="https://x.com/0xeman_raz" target="_blank">
                <img src="https://pbs.twimg.com/profile_images/2060406047391559681/sA9zPNKM_400x400.jpg" alt="Eman Raz">
                Built by Eman Raz
            </a>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# =====================================================
# Constants
# =====================================================
API_BASE = "https://api.axelarscan.io"

# =====================================================
# API Functions
# =====================================================
@st.cache_data(ttl=3600)
def get_its_assets():
    r = requests.get(f"{API_BASE}/api/getITSAssets", timeout=30)
    r.raise_for_status()
    return r.json()

@st.cache_data(ttl=3600)
def get_gateway_assets():
    r = requests.get(f"{API_BASE}/api/getAssets", timeout=30)
    r.raise_for_status()
    return r.json()

@st.cache_data(ttl=1800)
def get_chart_data(endpoint, asset, from_time, to_time):

    url = (
        f"{API_BASE}{endpoint}"
        f"?asset={asset}"
        f"&fromTime={from_time}"
        f"&toTime={to_time}"
    )

    try:
        resp = requests.get(url, timeout=60)

        if resp.status_code != 200:
            return pd.DataFrame()

        data = resp.json()

        if isinstance(data, dict) and "data" in data:
            return pd.DataFrame(data["data"])

        if isinstance(data, list):
            return pd.DataFrame(data)

    except Exception:
        pass

    return pd.DataFrame()

# =====================================================
# Timestamp Helper
# =====================================================
def convert_timestamp_series(s):

    s_num = pd.to_numeric(s, errors="coerce")

    if s_num.dropna().empty:
        return pd.to_datetime(s, errors="coerce", utc=True)

    maxv = s_num.max()

    if maxv > 1e17:
        unit = "ns"
    elif maxv > 1e14:
        unit = "us"
    elif maxv > 1e11:
        unit = "ms"
    else:
        unit = "s"

    try:
        return pd.to_datetime(
            s_num,
            unit=unit,
            errors="coerce",
            utc=True
        )
    except Exception:
        return pd.to_datetime(
            s,
            errors="coerce",
            utc=True
        )

# =====================================================
# Data Cleaning
# =====================================================
def sanitize_chart_df(df):

    if df is None or df.empty:
        return pd.DataFrame()

    for col in ["num_txs", "volume"]:

        if col in df.columns:
            df[col] = (
                pd.to_numeric(df[col], errors="coerce")
                .fillna(0)
                .astype(float)
            )
        else:
            df[col] = 0.0

    if "timestamp" not in df.columns:
        return pd.DataFrame()

    df["timestamp"] = convert_timestamp_series(df["timestamp"])

    df = df.dropna(subset=["timestamp"])

    return df[["timestamp", "num_txs", "volume"]]

# =====================================================
# UI
# =====================================================
st.title("💎 Asset Analysis")

st.info(
    "📊 Charts initially display data for a default time range. "
    "Select a custom range to view results for your desired period."
)

st.info(
    "⏳ On-chain data retrieval may take a few moments. "
    "Please wait while the results load."
)

# =====================================================
# Filters
# =====================================================
col1, col2, col3 = st.columns(3)

with col1:
    start_date = st.date_input(
        "Start Date",
        datetime.utcnow() - timedelta(days=30)
    )

with col2:
    end_date = st.date_input(
        "End Date",
        datetime.utcnow()
    )

with col3:
    timeframe = st.selectbox(
        "Timeframe (aggregation)",
        ["day", "week", "month"]
    )

freq_map = {
    "day": "D",
    "week": "W",
    "month": "M"
}

freq = freq_map[timeframe]

from_time = int(
    datetime.combine(
        start_date,
        datetime.min.time()
    ).timestamp()
)

to_time = int(
    datetime.combine(
        end_date,
        datetime.min.time()
    ).timestamp()
)

# =====================================================
# Assets
# =====================================================
gateway_assets = get_gateway_assets()
its_assets = get_its_assets()

gateway_df = pd.DataFrame(gateway_assets)

if gateway_df.empty:
    gateway_df = pd.DataFrame(
        columns=[
            "id",
            "denom",
            "native_chain",
            "name",
            "symbol",
            "decimals",
            "image",
            "coingecko_id",
            "addresses",
            "type",
        ]
    )
else:
    gateway_df["type"] = "Gateway"

its_df = pd.DataFrame(its_assets)

if its_df.empty:
    its_df = pd.DataFrame(
        columns=[
            "id",
            "symbol",
            "decimals",
            "image",
            "coingecko_id",
            "addresses",
            "type",
        ]
    )
else:
    its_df["type"] = "ITS"

all_tokens = pd.concat(
    [gateway_df, its_df],
    ignore_index=True,
    sort=False,
)

if "image" in all_tokens.columns:
    all_tokens = all_tokens.drop(columns=["image"])

all_tokens.index = range(1, len(all_tokens) + 1)

st.subheader("📋 Details of Supported Tokens")
st.dataframe(all_tokens, use_container_width=True)

# =====================================================
# Fetch Chart Data
# =====================================================
results = []

total = len(gateway_df) + len(its_df)
count = 0

progress = st.progress(0)

for _, token in gateway_df.iterrows():

    denom = token.get("denom")
    symbol = token.get("symbol") or denom

    raw = get_chart_data(
        "/token/transfersChart",
        denom,
        from_time,
        to_time,
    )

    df = sanitize_chart_df(raw)

    if not df.empty:
        df["token"] = symbol
        df["type"] = "Gateway"
        results.append(df)

    count += 1
    progress.progress(min(int(count / total * 100), 100))

for _, token in its_df.iterrows():

    symbol = token.get("symbol")

    raw = get_chart_data(
        "/gmp/GMPChart",
        symbol,
        from_time,
        to_time,
    )

    df = sanitize_chart_df(raw)

    if not df.empty:
        df["token"] = symbol
        df["type"] = "ITS"
        results.append(df)

    count += 1
    progress.progress(min(int(count / total * 100), 100))

# =====================================================
# Validation
# =====================================================
if not results:
    st.warning(
        "No time-series data available for the selected period."
    )
    st.stop()

# =====================================================
# Main Dataset
# =====================================================
full_df = pd.concat(
    results,
    ignore_index=True,
    sort=False
)

full_df["num_txs"] = (
    pd.to_numeric(full_df["num_txs"], errors="coerce")
    .fillna(0)
    .astype(float)
)

full_df["volume"] = (
    pd.to_numeric(full_df["volume"], errors="coerce")
    .fillna(0)
    .astype(float)
)

full_df = full_df.set_index("timestamp")

# XRP adjustment
mask_xrp = (
    full_df["token"]
    .astype(str)
    .str.upper()
    .eq("XRP")
)

full_df.loc[
    mask_xrp,
    ["num_txs", "volume"]
] = (
    full_df.loc[
        mask_xrp,
        ["num_txs", "volume"]
    ] / 2.0
)

# =====================================================
# Aggregation
# =====================================================
grouped = (
    full_df
    .groupby(["token", "type"])
    .resample(freq)[["num_txs", "volume"]]
    .sum()
    .reset_index()
)

# =====================================================
# Charts
# =====================================================
st.subheader("Number of Transfers by Token Over Time")

fig = px.bar(
    grouped,
    x="timestamp",
    y="num_txs",
    color="token",
    barmode="stack",
)

st.plotly_chart(fig, use_container_width=True)

st.subheader("Volume of Transfers by Token Over Time")

fig = px.bar(
    grouped,
    x="timestamp",
    y="volume",
    color="token",
    barmode="stack",
)

st.plotly_chart(fig, use_container_width=True)

totals = (
    grouped
    .groupby("token")[["num_txs", "volume"]]
    .sum()
    .reset_index()
)

totals = totals.sort_values(
    "volume",
    ascending=False
)

col1, col2 = st.columns(2)

with col1:

    transfers_sorted = totals.sort_values(
        "num_txs",
        ascending=False
    )

    fig = px.bar(
        transfers_sorted,
        x="num_txs",
        y="token",
        orientation="h"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )

with col2:

    fig = px.bar(
        totals,
        x="volume",
        y="token",
        orientation="h"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )

agg_type_time = (
    full_df
    .groupby("type")
    .resample(freq)[["num_txs", "volume"]]
    .sum()
    .reset_index()
)

st.subheader("Number of Transfers by ITS vs Gateway Over Time")

fig = px.bar(
    agg_type_time,
    x="timestamp",
    y="num_txs",
    color="type",
    barmode="stack",
)

st.plotly_chart(fig, use_container_width=True)

st.subheader("Volume of Transfers by ITS vs Gateway Over Time")

fig = px.bar(
    agg_type_time,
    x="timestamp",
    y="volume",
    color="type",
    barmode="stack",
)

st.plotly_chart(fig, use_container_width=True)

agg_type = (
    grouped
    .groupby("type")[["num_txs", "volume"]]
    .sum()
    .reset_index()
)

col1, col2 = st.columns(2)

with col1:
    fig = px.pie(
        agg_type,
        names="type",
        values="num_txs",
        hole=0.3
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.pie(
        agg_type,
        names="type",
        values="volume",
        hole=0.3
    )
    st.plotly_chart(fig, use_container_width=True)

col1, col2 = st.columns(2)

with col1:
    st.dataframe(
        totals.head(20),
        use_container_width=True
    )

with col2:
    st.dataframe(
        transfers_sorted.head(20),
        use_container_width=True
    )

st.success("Dashboard ready ✅")
