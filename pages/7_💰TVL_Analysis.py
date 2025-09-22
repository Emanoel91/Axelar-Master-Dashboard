import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
import streamlit as st

# --- Page Config: Tab Title & Icon ---
st.set_page_config(
    page_title="Axelar Master Dashboard",
    page_icon="https://axelarscan.io/logos/logo.png",
    layout="wide"
)

# reading data-------------------------------------------------------
df = pd.read_csv("tvl_data.csv")

# convert date to supported date format -------------------------------------------
df["date"] = pd.to_datetime(df["date"], format="mixed", errors="coerce")
df = df.dropna(subset=["date"])

# Data type assurance ---------------------------------------
df["tvl"] = pd.to_numeric(df["tvl"], errors="coerce")
df = df.dropna(subset=["tvl"])

# Data sorting
df = df.sort_values("date")

# --- Title with Logo ---------------------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="display: flex; align-items: center; gap: 15px;">
        <img src="https://axelarscan.io/logos/chains/axelarnet.svg" alt="Axelar Logo" style="width:60px; height:60px;">
        <h1 style="margin: 0;">Axelar TVL Monitoring: ITS vs. Non-ITS</h1>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Reference and Rebuild Info --------------------------------------------------------------------------------------
st.markdown(
    """
    
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://pbs.twimg.com/profile_images/1841479747332608000/bindDGZQ_400x400.jpg" alt="Eman Raz" style="width:25px; height:25px; border-radius: 50%;">
            <span>Built by: <a href="https://x.com/0xeman_raz" target="_blank">Eman Raz</a></span>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)
# --- Row 1: Stacked Bar Chart ------------------

st.info(
    "🔔The TVL data for the Axelar network is updated every 24 hours."

)

# Plot order for columns (ITS is placed above non-ITS) --------------------------------
category_order = {"asset_type": ["non-ITS", "ITS"]}

fig1 = px.bar(
    df,
    x="date",
    y="tvl",
    color="asset_type",
    title="Axelar TVL Over Time",
    labels={"tvl": "TVL ($USD)", "date": "Date"},
    category_orders=category_order,
)

# Calculate total daily TVL ---------------------
daily_total = df.groupby("date")["tvl"].sum().reset_index()

# Add TVL_total line on y-axis -----------------------------
fig1.add_trace(
    go.Scatter(
        x=daily_total["date"],
        y=daily_total["tvl"],
        mode="lines",
        name="Total TVL",
        line=dict(color="black", width=2),
        yaxis="y"
    )
)

st.plotly_chart(fig1, use_container_width=True)

# --- Row 2: Normalized Area Chart --------------------------------

df_grouped = df.groupby(["date", "asset_type"])["tvl"].sum().reset_index()
fig2 = px.area(
    df_grouped,
    x="date",
    y="tvl",
    color="asset_type",
    groupnorm="fraction",
    title="Percentage Share of ITS and Non-ITS Assets in TVL Over Time (%)",
)
st.plotly_chart(fig2, use_container_width=True)

# --- Row3: Donut chart و KPI ---------------------------------
st.subheader("Latest Day TVL Breakdown")
latest_date = df["date"].max()
latest_df = df[df["date"] == latest_date]
total_tvl = latest_df["tvl"].sum()

# Calculate the previous day's TVL ----------------------
prev_date = latest_date - pd.Timedelta(days=1)
prev_df = df[df["date"] == prev_date]
prev_total_tvl = prev_df["tvl"].sum() if not prev_df.empty else None

# Calculate the percentage change ---------------------
if prev_total_tvl and prev_total_tvl > 0:
    change_pct = (total_tvl - prev_total_tvl) / prev_total_tvl * 100
else:
    change_pct = None

col1, col2 = st.columns(2)
with col1:
    fig3 = px.pie(
        latest_df,
        names="asset_type",
        values="tvl",
        hole=0.5,
        title=f"TVL by Asset Type ({latest_date.date()})",
    )
    st.plotly_chart(fig3, use_container_width=True)

with col2:
    st.markdown("### Axelar Current TVL")
    st.markdown(f"<h1 style='margin: 0;'>${total_tvl:,.0f}</h1>", unsafe_allow_html=True)
    if change_pct is not None:
        color = "green" if change_pct > 0 else "red"
        sign = "+" if change_pct > 0 else ""
        st.markdown(
            f"<p style='color:{color}; font-size: 24px; margin: 0;'>{sign}{change_pct:.2f}% in 24h</p>", 
            unsafe_allow_html=True
        )
    else:
        st.markdown("<p style='color:gray;'>No data for previous day</p>", unsafe_allow_html=True)

# --- Row4: Area Chart ---
st.subheader("Monthly TVL Stats")
df_monthly = df.copy()
df_monthly["month"] = df_monthly["date"].dt.to_period("M")
monthly_stats = df_monthly.groupby(["month", "asset_type"])["tvl"].agg(
    ["max", "mean", "min"]
).reset_index()
monthly_stats["month"] = monthly_stats["month"].dt.to_timestamp()

col1, col2, col3 = st.columns(3)
with col1:
    fig_max = px.area(
        monthly_stats,
        x="month",
        y="max",
        color="asset_type",
        title="Maximum TVL per Month",
    )
    st.plotly_chart(fig_max, use_container_width=True)

with col2:
    fig_avg = px.area(
        monthly_stats,
        x="month",
        y="mean",
        color="asset_type",
        title="Average TVL per Month",
    )
    st.plotly_chart(fig_avg, use_container_width=True)

with col3:
    fig_min = px.area(
        monthly_stats,
        x="month",
        y="min",
        color="asset_type",
        title="Minimum TVL per Month",
    )
    st.plotly_chart(fig_min, use_container_width=True)

# --- Links with Logos ---------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="font-size: 16px;">
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://axelarscan.io/logos/logo.png" alt="Axelar" style="width:20px; height:20px;">
            <a href="https://www.axelar.network/" target="_blank">Axelar Website</a>
        </div>
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://axelarscan.io/logos/logo.png" alt="Axelar" style="width:20px; height:20px;">
            <a href="https://x.com/axelar" target="_blank">Axelar X Account</a>
        </div>
        
    </div>
    """,
    unsafe_allow_html=True
)
