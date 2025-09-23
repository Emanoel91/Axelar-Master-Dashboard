import streamlit as st
import pandas as pd
import requests
import snowflake.connector
import plotly.graph_objects as go
import plotly.express as px
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# --- Page Config: Tab Title & Icon -------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Axelar Master Dashboard",
    page_icon="https://axelarscan.io/logos/logo.png",
    layout="wide"
)

# --- Sidebar Footer Slightly Left-Aligned ---
st.sidebar.markdown(
    """
    <style>
    .sidebar-footer {
        position: fixed;
        bottom: 20px;
        width: 250px;
        font-size: 13px;
        color: gray;
        margin-left: 5px;
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
                <img src="https://pbs.twimg.com/profile_images/1841479747332608000/bindDGZQ_400x400.jpg" alt="Eman Raz">
                Built by Eman Raz
            </a>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Title & Info Messages ---------------------------------------------------------------------------------------------
st.title("⛓Axelar Network")
st.info("⏳ On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Snowflake Connection ----------------------------------------------------------------------------------------
snowflake_secrets = st.secrets["snowflake"]
user = snowflake_secrets["user"]
account = snowflake_secrets["account"]
private_key_str = snowflake_secrets["private_key"]
warehouse = snowflake_secrets.get("warehouse", "")
database = snowflake_secrets.get("database", "")
schema = snowflake_secrets.get("schema", "")

private_key_pem = f"-----BEGIN PRIVATE KEY-----\n{private_key_str}\n-----END PRIVATE KEY-----".encode("utf-8")
private_key = serialization.load_pem_private_key(
    private_key_pem,
    password=None,
    backend=default_backend()
)
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

conn = snowflake.connector.connect(
    user=user,
    account=account,
    private_key=private_key_bytes,
    warehouse=warehouse,
    database=database,
    schema=schema
)

st.markdown(
    """
    <div style="background-color:#ff7f27; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Axelar Network's User Retention</h2>
    </div>
    """,
    unsafe_allow_html=True
)
st.markdown("<br>", unsafe_allow_html=True)

# --- Row 1: User Retention -------------------------------------------------------------------
@st.cache_data
def load_user_retention():
    query = """
    with base as (
      select
          TX_FROM as TX_SIGNER,
          min(date_trunc('month', BLOCK_TIMESTAMP)) over (partition by TX_SIGNER) as signup_date,
          date_trunc('month', BLOCK_TIMESTAMP) as activity_date,
          datediff('month', signup_date, activity_date) as difference
      from axelar.core.fact_transactions
    ),
    unp as (
      select
        TO_VARCHAR(signup_date, 'yyyy-MM') as cohort_date,
        difference as months,
        count(distinct TX_SIGNER) as users
      from base
      where datediff('month', signup_date, current_date()) <= 24
      group by 1,2
    ),
    fine as (
      select
        u.*,
        p.users as user0
      from unp u
      left join unp p on u.cohort_date = p.cohort_date
      where p.months = 0
    )
    select
      cohort_date as "Cohort Date",
      months as "Month",
      round(100 * users / user0, 2) as "Retention Rate"
    from fine
    where round(100 * users / user0, 2) <> 100
    order by 1 desc, 2
    """
    df = pd.read_sql(query, conn)
    return df

# === Load Data: Row 1 ====================================
df_user_retention = load_user_retention()

# === Chart: Heatmap (Row 1) ==============================
pivot_tt_users = df_user_retention.pivot_table(
    index="Cohort Date", columns="Month", values="Retention Rate",
    aggfunc="sum", fill_value=0
)
fig_heatmap_tt_users = px.imshow(
    pivot_tt_users, text_auto=True, aspect="auto",
    color_continuous_scale='Viridis', title="Axelar Network: User Retention"
)
st.plotly_chart(fig_heatmap_tt_users, use_container_width=True)

# --- Row 2: TPS & Success Rate KPIs ----------------------------------------------------------------
@st.cache_data
def load_user_stats_tps():
    query = """
     select 
        date_trunc('week', BLOCK_TIMESTAMP) as date,
        round((sum(TX_COUNT)/(7*24*3600)),2) as TPS,
        round(100*(TPS-lag(TPS,1) over(order by date))/lag(TPS,1) over(order by date),2) as "TPS Change%"
     from axelar.core.fact_blocks
     where block_timestamp::date < current_date
     group by 1
     qualify row_number() over(order by date desc) > 1
     order by 1 desc
     limit 1
    """
    df = pd.read_sql(query, conn)
    return df

@st.cache_data
def load_user_stats_success_rate():
    query = """
    select 
        date_trunc('week', BLOCK_TIMESTAMP)::date as date,
        count(*) as TX,
        sum(case when TX_SUCCEEDED!='TRUE' then 0 else 1 end) as "Success TX",
        (sum(case when TX_SUCCEEDED!='TRUE' then 0 else 1 end)/count(*))*100 as "Success %",
        round((100 - (sum(case when TX_SUCCEEDED!='TRUE' then 0 else 1 end)/count(*))*100),2) as failure_rate,
        round(100*(( (sum(case when TX_SUCCEEDED!='TRUE' then 0 else 1 end)/count(*))*100 )-lag((sum(case when TX_SUCCEEDED!='TRUE' then 0 else 1 end)/count(*))*100,1) over(order by date))/
             lag((sum(case when TX_SUCCEEDED!='TRUE' then 0 else 1 end)/count(*))*100,1) over(order by date),2) as "Success rate change %"
    from axelar.core.fact_transactions
    where block_timestamp::date between current_date - interval '1 year' and current_date - 1
    group by 1 
    order by 1 desc
    limit 1
    """
    df = pd.read_sql(query, conn)
    return df

# === Load Data ===========================================
df_user_stats_tps = load_user_stats_tps()
df_user_stats_success_rate = load_user_stats_success_rate()

# === KPIs: Row 2 =========================================
card_style = """
    <div style="
        background-color: #ffffff;
        border: 1px solid #ffffff;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.05);
        ">
        <h4 style="margin: 0; font-size: 20px; color: #000000;">{label}</h4>
        <p style="margin: 5px 0 0; font-size: 20px; font-weight: bold; color: #000000;">{value}</p>
    </div>
"""

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(card_style.format(label="TPS Past Week", value=f"{df_user_stats_tps['TPS'][0]:,} Txns"), unsafe_allow_html=True)
with col2:
    st.markdown(card_style.format(label="Weekly Change in TPS", value=f"{df_user_stats_tps['TPS Change%'][0]:,}%"), unsafe_allow_html=True)
with col3:
    st.markdown(card_style.format(label="Success Rate", value=f"{df_user_stats_success_rate['Success %'][0]:,}%"), unsafe_allow_html=True)
with col4:
    st.markdown(card_style.format(label="Weekly Change in Success Rate", value=f"{df_user_stats_success_rate['Success rate change %'][0]:,}%"), unsafe_allow_html=True)

# --- Row 3: Weekly TPS & Success Rate Trends ------------------------------------------------------
@st.cache_data
def load_weekly_tps():
    query = """
     select 
        date_trunc('week', BLOCK_TIMESTAMP) as "Date",
        round((sum(TX_COUNT)/(7*24*3600)),2) as TPS,
        round(100*(TPS-lag(TPS,1) over(order by "Date"))/lag(TPS,1) over(order by "Date"),2) as "TPS Change %"
     from axelar.core.fact_blocks
     where block_timestamp < current_date 
     group by 1
     qualify row_number() over(order by "Date" desc) > 1
     order by 1 desc
    """
    df = pd.read_sql(query, conn)
    return df

@st.cache_data
def load_weekly_success_rate():
    query = """
      select 
      date_trunc('week', BLOCK_TIMESTAMP)::date AS "Date",
      count(*) AS TX,
      sum(case when TX_SUCCEEDED!='TRUE' then 0 else 1 end) AS "Success TX",
      ("Success TX"/TX)*100 AS "Success %",
      round((100-"Success %"),2) as failure_rate ,
      round ( 100*("Success %"-lag("Success %",1)over(order by "Date"))/lag("Success %",1)over(order by "Date"),2) as  "Success Rate Change %"
      from axelar.core.fact_transactions
      where BLOCK_TIMESTAMP::date between current_date - interval ' 1 year ' and current_date -1
      group by 1 
      order by 1 desc

    """
    df = pd.read_sql(query, conn)
    return df

# === Load Data: Row 3 ========================================================
df_weekly_tps = load_weekly_tps()
df_weekly_success_rate = load_weekly_success_rate()

# === Charts: Row 3 ============================================================
col1, col2 = st.columns(2)

with col1:
    fig1 = go.Figure()
    fig1.add_bar(x=df_weekly_tps["Date"], y=df_weekly_tps["TPS"], name="TPS", yaxis="y1", marker_color="blue")
    fig1.add_trace(go.Scatter(x=df_weekly_tps["Date"], y=df_weekly_tps["TPS Change %"], name="TPS Change %", mode="lines", 
                              yaxis="y2", line=dict(color="black")))
    fig1.update_layout(title="Weekly Average TPS", 
                       yaxis=dict(title="Txns count"), 
                       yaxis2=dict(title="%", overlaying="y", side="right"), 
                       xaxis=dict(title=""),
                       barmode="group", 
                       legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = go.Figure()
    fig2.add_bar(x=df_weekly_success_rate["Date"], y=df_weekly_success_rate["Success %"], name="Success %", yaxis="y1", marker_color="blue")
    fig2.add_trace(go.Scatter(x=df_weekly_success_rate["Date"], y=df_weekly_success_rate["Success Rate Change %"], name="Success Rate Change %", mode="lines", 
                              yaxis="y2", line=dict(color="black")))
    fig2.update_layout(title="Weekly Success Rate", 
                       yaxis=dict(title="%"), 
                       yaxis2=dict(title="%", overlaying="y", side="right"), 
                       xaxis=dict(title=""),
                       barmode="group", 
                       legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
    st.plotly_chart(fig2, use_container_width=True)
