import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.graph_objects as go
import plotly.express as px
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# --- Page Config ------------------------------------------------------------------------------------------------------
st.set_page_config(page_title="Axelar Master Dashboard", page_icon="https://axelarscan.io/logos/logo.png", layout="wide")
st.title("🥩AXL Staking")
st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

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

# --- Date Inputs ---------------------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)
with col1:
    timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"])
with col2:
    start_date = st.date_input("Start Date", value=pd.to_datetime("2022-08-01"))
with col3:
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-09-30"))

st.markdown(
    """
    <div style="background-color:#ff7f27; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Share of Staked Tokens from Supply</h2>
    </div>
    """,
    unsafe_allow_html=True
)
st.markdown("<br>", unsafe_allow_html=True)
# --- Query Functions -----------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_share_of_staked_tokens(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    query = f"""
        WITH delegate AS (
            SELECT
                TRUNC(block_timestamp,'month') AS monthly, 
                SUM(amount/POW(10,6)) AS delegate_amount,
                SUM(SUM(amount/POW(10,6))) OVER (ORDER BY TRUNC(block_timestamp,'month') ASC) AS cumulative_delegate_amount
            FROM axelar.gov.fact_staking
            WHERE action = 'delegate'
              AND TX_SUCCEEDED = 'TRUE'
              AND block_timestamp::date >= '{start_date}'
              AND block_timestamp::date <= '{end_date}'
            GROUP BY 1
        ),
        undelegate AS (
            SELECT
                TRUNC(block_timestamp,'month') AS monthly, 
                SUM(amount/POW(10,6)) * -1 AS undelegate_amount,
                SUM(SUM(amount/POW(10,6)) * -1) OVER (ORDER BY TRUNC(block_timestamp,'month') ASC) AS cumulative_undelegate_amount
            FROM axelar.gov.fact_staking
            WHERE action = 'undelegate'
              AND TX_SUCCEEDED = 'TRUE'
              AND block_timestamp::date >= '{start_str}'
              AND block_timestamp::date <= '{end_str}'
            GROUP BY 1
        )
        SELECT 
            (cumulative_delegate_amount + cumulative_undelegate_amount) / 1008585017 * 100 AS share_of_staked_tokens
        FROM delegate a
        LEFT OUTER JOIN undelegate b
          ON a.monthly = b.monthly
        WHERE a.monthly >= '{start_str}'
        ORDER BY a.monthly DESC
        LIMIT 1
    """
    df = pd.read_sql(query, conn)
    if not df.empty:
        return round(df["SHARE_OF_STAKED_TOKENS"].iloc[0], 2)
    else:
        return None

# --- Row2: Monthly Share Chart ---
@st.cache_data
def load_monthly_share_data(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    query = f"""
        WITH delegate AS (
            SELECT 
                TRUNC(block_timestamp,'month') AS monthly, 
                SUM(amount/POW(10,6)) AS delegate_amount,
                SUM(SUM(amount/POW(10,6))) OVER (ORDER BY TRUNC(block_timestamp,'month') ASC) AS cumulative_delegate_amount,
                COUNT(DISTINCT tx_id) AS delegate_tx,
                COUNT(DISTINCT DELEGATOR_ADDRESS) AS delegate_user,
                AVG(amount/POW(10,6)) AS avg_delegate_amount 
            FROM axelar.gov.fact_staking
            WHERE action = 'delegate'
              AND block_timestamp::date >= '{start_date}'
              AND block_timestamp::date <= '{end_date}'
            GROUP BY 1
        ),
        undelegate AS (
            SELECT 
                TRUNC(block_timestamp,'month') AS monthly, 
                SUM(amount/POW(10,6)) * -1 AS undelegate_amount,
                SUM(SUM(amount/POW(10,6)) * -1) OVER (ORDER BY TRUNC(block_timestamp,'month') ASC) AS cumulative_undelegate_amount,
                COUNT(DISTINCT tx_id) * -1 AS undelegate_tx,
                COUNT(DISTINCT DELEGATOR_ADDRESS) * -1 AS undelegate_user,
                AVG(amount/POW(10,6)) AS avg_undelegate_amount 
            FROM axelar.gov.fact_staking
            WHERE action = 'undelegate'
              AND block_timestamp::date >= '{start_date}'
              AND block_timestamp::date <= '{end_date}'
            GROUP BY 1
        )
        SELECT 
            a.monthly, 
            delegate_amount,
            undelegate_amount,
            cumulative_delegate_amount,
            cumulative_undelegate_amount,
            delegate_tx,
            undelegate_tx,
            delegate_user,
            undelegate_user,
            1008585017 AS supply,
            cumulative_delegate_amount + cumulative_undelegate_amount AS net,
            (cumulative_delegate_amount + cumulative_undelegate_amount) / 1008585017 * 100 AS "Share of Staked Tokens From Supply"
        FROM delegate a
        LEFT OUTER JOIN undelegate b ON a.monthly = b.monthly 
        WHERE a.monthly >= '{start_date}' AND a.monthly <= '{end_date}'
        ORDER BY 1 ASC
    """
    return pd.read_sql(query, conn)
 
# --- Load Data ---------------------------------------------------------------------------------------------------------------------------------------------------------------
share_of_staked_tokens = load_share_of_staked_tokens(start_date, end_date)
monthly_share_df = load_monthly_share_data(start_date, end_date)
# --- Row 1: KPI ---------------------------------------------------------------------------------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="background-color:#fc0060; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Share of Staked Tokens from Supply</h2>
    </div>
    """,
    unsafe_allow_html=True
)
if share_of_staked_tokens is not None:
    st.metric("Share of Staked Tokens From Supply", f"{share_of_staked_tokens:.2f}%")
else:
    st.warning("No data available for the selected period.")

# --- Row 2: Monthly Share of Staked Tokens from Supply Chart -------------------------
if not monthly_share_df.empty:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=monthly_share_df['MONTHLY'],
        y=monthly_share_df['Share of Staked Tokens From Supply'],
        mode='markers+lines',
        marker=dict(size=8, color='blue'),
        line=dict(color='blue', width=2)
    ))
    fig.update_layout(
        title="Monthly Share of Staked Tokens from Supply",
        xaxis_title="Month",
        yaxis_title="Share (%)",
        hovermode='x unified',
        template='plotly_white',
        height=500
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No monthly data available for the selected period.")

