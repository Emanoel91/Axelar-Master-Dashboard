import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import requests
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

# --- Get Total Supply from API ----------------------------------------------------------------------------------------
@st.cache_data
def get_total_supply():
    url = "https://api.axelarscan.io/api/getTotalSupply"
    response = requests.get(url)
    response.raise_for_status()
    supply = float(response.text.strip())
    return round(supply)  # رند کردن به عدد صحیح برای نمایش در KPI

CURRENT_TOTAL_SUPPLY = get_total_supply()

# --- Date Inputs ---------------------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)
with col1:
    timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"])
with col2:
    start_date = st.date_input("Start Date", value=pd.to_datetime("2022-09-01"))
with col3:
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-09-30"))

st.markdown(
    """
    <div style="background-color:#ff7f27; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">AXL Stking Overview</h2>
    </div>
    """,
    unsafe_allow_html=True
)
st.markdown("<br>", unsafe_allow_html=True)

# --- Row 1 ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_current_net_staked(total_supply):
    query = f"""
    with date_start as (
    with dates AS (
    SELECT CAST('2022-02-10' AS DATE) AS start_date 
    UNION ALL
    SELECT DATEADD(day, 1, start_date)
    FROM dates
    WHERE start_date < CURRENT_DATE())
    SELECT date_trunc(day, start_date) AS start_date
    FROM dates),
    axl_stakers_balance_change as (
    select * from 
        (select date_trunc(day, block_timestamp) as date, 
        user, 
        sum(amount)/1e6 as balance_change
        from 
            (
            select block_timestamp, DELEGATOR_ADDRESS as user, -1* amount as amount, TX_ID as tx_hash
            from axelar.gov.fact_staking
            where action='undelegate' and TX_SUCCEEDED=TRUE
            union all 
            select block_timestamp, DELEGATOR_ADDRESS, amount, TX_ID
            from axelar.gov.fact_staking
            where action='delegate' and TX_SUCCEEDED=TRUE)
        group by 1,2)),

    axl_stakers_historic_holders as (
    select user
    from axl_stakers_balance_change
    group by 1),

    user_dates as (
    select start_date, user
    from date_start, axl_stakers_historic_holders),

    users_balance as 
    (select start_date as "Date", user,
    lag(balance_raw) ignore nulls over (partition by user order by start_date) as balance_lag,
    ifnull(balance_raw, balance_lag) as balance
    from (
        select start_date, a.user, balance_change,
        sum(balance_change) over (partition by a.user order by start_date) as balance_raw,
        from user_dates a 
        left join axl_stakers_balance_change b 
        on date=start_date and a.user=b.user))

    select "Date", round(sum(balance)) as "Net Staked", {total_supply} as "Current Total Supply",
           round((100*"Net Staked"/{total_supply}),2) as "Net Staked %"
    from users_balance
    where balance>=0.001 and balance is not null
    group by 1 
    order by 1 desc
    limit 1
    """
    df = pd.read_sql(query, conn)
    return df

# --- Load Data: Row --------------------------------------------------------------------------------------------------------
df_current_net_staked = load_current_net_staked(CURRENT_TOTAL_SUPPLY)

# --- KPIs: Row 1 ---------------------------------------------------------------------------------------------------
card_style = """
    <div style="
        background-color: #f9f9f9;
        border: 1px solid #e0e0e0;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.05);
        ">
        <h4 style="margin: 0; font-size: 20px; color: #555;">{label}</h4>
        <p style="margin: 5px 0 0; font-size: 20px; font-weight: bold; color: #000;">{value}</p>
    </div>
"""

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(card_style.format(label="Current Net Staked", value=f"{df_current_net_staked['Net Staked'][0]:,} $AXL"), unsafe_allow_html=True)
with col2:
    st.markdown(card_style.format(label="%Staked-to-Total Supply", value=f"{df_current_net_staked['Net Staked %'][0]:,}%"), unsafe_allow_html=True)
with col3:
    st.markdown(card_style.format(label="Current Total Supply", value=f"{round(df_current_net_staked['Current Total Supply'][0]):,} $AXL"), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# --- Row 2 ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_net_staked_overtime(start_date, end_date, total_supply):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    query = f"""
    with overview as (
    with date_start as (
    with dates AS (
    SELECT CAST('2022-02-10' AS DATE) AS start_date 
    UNION ALL
    SELECT DATEADD(day, 1, start_date)
    FROM dates
    WHERE start_date < CURRENT_DATE())
    SELECT date_trunc(day, start_date) AS start_date
    FROM dates),
    axl_stakers_balance_change as (
    select * from 
        (select date_trunc(day, block_timestamp) as date, 
        user, 
        sum(amount)/1e6 as balance_change
        from 
            (
            select block_timestamp, DELEGATOR_ADDRESS as user, -1* amount as amount, TX_ID as tx_hash
            from axelar.gov.fact_staking
            where action='undelegate' and TX_SUCCEEDED=TRUE
            union all 
            select block_timestamp, DELEGATOR_ADDRESS, amount, TX_ID
            from axelar.gov.fact_staking
            where action='delegate' and TX_SUCCEEDED=TRUE)
        group by 1,2)),

    axl_stakers_historic_holders as (
    select user
    from axl_stakers_balance_change
    group by 1),

    user_dates as (
    select start_date, user
    from date_start, axl_stakers_historic_holders),

    users_balance as 
    (select start_date as "Date", user,
    lag(balance_raw) ignore nulls over (partition by user order by start_date) as balance_lag,
    ifnull(balance_raw, balance_lag) as balance
    from (
        select start_date, a.user, balance_change,
        sum(balance_change) over (partition by a.user order by start_date) as balance_raw,
        from user_dates a 
        left join axl_stakers_balance_change b 
        on date=start_date and a.user=b.user))

    select "Date", round(sum(balance)) as "Net Staked", {total_supply} as "Current Total Supply",
           round((100*"Net Staked"/{total_supply}),2) as "Net Staked %"
    from users_balance
    where balance>=0.001 and balance is not null
    group by 1 
    order by 1 desc)
    select "Date", "Net Staked"
    from overview
    where "Date">='{start_str}' and "Date"<='{end_str}'
    order by 1
    """
    df = pd.read_sql(query, conn)
    return df

# --- Load Data: Row 2 ----------------------------------------------------------------------------------------
df_net_staked_overtime = load_net_staked_overtime(start_date, end_date, CURRENT_TOTAL_SUPPLY)

# --- Charts 2 ------------------------------------------------------------------------------------------------
fig = px.area(df_net_staked_overtime, x="Date", y="Net Staked", title="AXL Net Staked Amount Over Time")
fig.update_layout(xaxis_title="", yaxis_title="$AXL", template="plotly_white")
st.plotly_chart(fig, use_container_width=True)

# --- Row 3 ---------------------------------------------------------------------------------------------------
@st.cache_data
def load_staking_stats(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with table1 as (
    select count(distinct tx_id) as "Staking Count",
    count(distinct delegator_address) as "Unique Stakers",
    75 as "Active Validators"
   from axelar.gov.fact_staking
   where tx_succeeded='true' and currency='uaxl' and block_timestamp::date>='{start_str}' AND
   block_timestamp::date<='{end_str}' and action='delegate')
   select * from table1
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data: Row --------------------------------------
df_staking_stats = load_staking_stats(start_date, end_date)
# --- KPIs: Row 3 ---------------------------------------------------------------------------------------------------------------------------------------------------------
card_style = """
    <div style="
        background-color: #f9f9f9;
        border: 1px solid #e0e0e0;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.05);
        ">
        <h4 style="margin: 0; font-size: 20px; color: #555;">{label}</h4>
        <p style="margin: 5px 0 0; font-size: 20px; font-weight: bold; color: #000;">{value}</p>
    </div>
"""

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(card_style.format(label="Staking Transactions", value=f"{df_staking_stats["Staking Count"][0]:,} Txns"), unsafe_allow_html=True)
with col2:
    st.markdown(card_style.format(label="Unique Stakers", value=f"{df_staking_stats["Unique Stakers"][0]:,} Wallets"), unsafe_allow_html=True)
with col3:
    st.markdown(card_style.format(label="Active Validators", value=f"{df_staking_stats["Active Validators"][0]:,}"), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)
