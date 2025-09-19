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
                <img src="https://pbs.twimg.com/profile_images/1841479747332608000/bindDGZQ_400x400.jpg" alt="Eman Raz">
                Built by Eman Raz
            </a>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Title & Info Messages ---------------------------------------------------------------------------------------------
st.title("🚀GMP & Token Transfers")

st.info("📊 Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
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

# --- Time Frame & Period Selection --------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)
with col1:
    timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"])
with col2:
    start_date = st.date_input("Start Date", value=pd.to_datetime("2022-01-01"))
with col3:
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-09-30"))

# --- Fetch Data from API --------------------------------------------------------------------------------------------
@st.cache_data
def load_data():
    url = "https://api.axelarscan.io/api/interchainChart"
    response = requests.get(url)
    json_data = response.json()
    df = pd.DataFrame(json_data['data'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

df = load_data()

# --- Filter by date range ------------------------------------------------------------------------------------------
df = df[(df['timestamp'] >= pd.to_datetime(start_date)) & (df['timestamp'] <= pd.to_datetime(end_date))]

# --- Resample data based on timeframe ------------------------------------------------------------------------------
if timeframe == "week":
    df['period'] = df['timestamp'].dt.to_period('W').apply(lambda r: r.start_time)
elif timeframe == "month":
    df['period'] = df['timestamp'].dt.to_period('M').apply(lambda r: r.start_time)
else:
    df['period'] = df['timestamp']

grouped = df.groupby('period').agg({
    'gmp_num_txs': 'sum',
    'gmp_volume': 'sum',
    'transfers_num_txs': 'sum',
    'transfers_volume': 'sum'
}).reset_index()

grouped['total_txs'] = grouped['gmp_num_txs'] + grouped['transfers_num_txs']
grouped['total_volume'] = grouped['gmp_volume'] + grouped['transfers_volume']

# --- Row 1, 2 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# === Number of Unique Chains ===========================
@st.cache_data
def load_unique_chains_stats(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
  
  SELECT LOWER(data:send:original_source_chain) AS chain, 
  FROM axelar.axelscan.fact_transfers
  WHERE status = 'executed' AND simplified_status = 'received'
    
  UNION ALL

  SELECT LOWER(data:send:original_destination_chain) AS chain
  FROM axelar.axelscan.fact_transfers
  WHERE status = 'executed' AND simplified_status = 'received'

  union all

  SELECT  LOWER(data:call.chain::STRING) AS chain
  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed' AND simplified_status = 'received'

  union all 

  SELECT LOWER(data:call.returnValues.destinationChain::STRING) AS chain
  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed' AND simplified_status = 'received')

SELECT count(distinct chain)-1 as "Unique Chains"
FROM axelar_service
where chain is not null
    """
    df = pd.read_sql(query, conn)
    return df

# === Axelar Cross-chain Stats =====================
@st.cache_data
def load_crosschain_stats(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
  SELECT 
    created_at, 
    LOWER(data:send:original_source_chain) AS source_chain, 
    LOWER(data:send:original_destination_chain) AS destination_chain,
    sender_address AS user, case 
      WHEN IS_ARRAY(data:send:fee_value) THEN NULL
      WHEN IS_OBJECT(data:send:fee_value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
      ELSE NULL END AS fee
  FROM axelar.axelscan.fact_transfers
  WHERE status = 'executed' AND simplified_status = 'received'

  UNION ALL

  SELECT  
    created_at,
    LOWER(data:call.chain::STRING) AS source_chain,
    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user, COALESCE( CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR    IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL END, CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL END) AS fee
  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed' AND simplified_status = 'received')

SELECT count(distinct user) as "Number of Users", round(sum(fee)) as "Total Gas Fees",
count(distinct (source_chain || '➡' || destination_chain)) as "Unique Paths",
round(avg(fee),2) as "Avg Gas Fee", round(median(fee),2) as "Median Gas Fee"
from axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
    """
    df = pd.read_sql(query, conn)
    return df

# === Load Kpi =====================================
df_unique_chains_stats = load_unique_chains_stats(start_date, end_date)
df_crosschain_stats = load_crosschain_stats(start_date, end_date)
# --- KPI Section ---------------------------------------------------------------------------------------------------
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

total_num_txs = grouped['gmp_num_txs'].sum() + grouped['transfers_num_txs'].sum()
total_volume = grouped['gmp_volume'].sum() + grouped['transfers_volume'].sum()

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(card_style.format(label="🚀Number of Transfers", value=f"{total_num_txs:,} Txns"), unsafe_allow_html=True)
with col2:
    st.markdown(card_style.format(label="💸Volume of Transfers", value=f"${total_volume:,.0f}"), unsafe_allow_html=True)
with col3:
    st.markdown(card_style.format(label="👥Number of Unique Users", value=f"{df_crosschain_stats['Number of Users'][0]:,} Wallets"), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

col4, col5, col6 = st.columns(3)
with col4:
    st.markdown(card_style.format(label="⛓Supported Chains", value=f"{df_unique_chains_stats['Unique Chains'][0]:,}"), unsafe_allow_html=True)
with col5:
    st.markdown(card_style.format(label="🔀Number of Paths", value=f"{df_crosschain_stats['Unique Paths'][0]:,}"), unsafe_allow_html=True)
with col6:
    st.markdown(card_style.format(label="⛽Total Gas Fees", value=f"${df_crosschain_stats['Total Gas Fees'][0]:,}"), unsafe_allow_html=True)
    
# --- Row 3: Transactions Over Time -------------------------------------------------------------------------------------------------------------------------------------------
fig1 = go.Figure()
fig1.add_trace(go.Bar(x=grouped['period'], y=grouped['gmp_num_txs'], name='GMP', marker_color='#ff7400'))
fig1.add_trace(go.Bar(x=grouped['period'], y=grouped['transfers_num_txs'], name='Token Transfers', marker_color='#00a1f7'))
fig1.add_trace(go.Scatter(x=grouped['period'], y=grouped['total_txs'], name='Total', mode='lines', marker_color='black'))
fig1.update_layout(barmode='stack', title="Number of Transfers by Service Over Time", yaxis=dict(title="Txns count"), 
                   legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))

fig2 = go.Figure()
fig2.add_trace(go.Bar(x=grouped['period'], y=grouped['gmp_volume'], name='GMP', marker_color='#ff7400'))
fig2.add_trace(go.Bar(x=grouped['period'], y=grouped['transfers_volume'], name='Token Transfers', marker_color='#00a1f7'))
fig2.add_trace(go.Scatter(x=grouped['period'], y=grouped['total_volume'], name='Total', mode='lines', marker_color='black'))
fig2.update_layout(barmode='stack', title="Volume of Transfers by Service Over Time", yaxis=dict(title="$USD"), 
                   legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))

col1, col2 = st.columns(2)

with col1:
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.plotly_chart(fig2, use_container_width=True)

# --- Row 4 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_stats_overtime(timeframe, start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
  SELECT 
    created_at, 
    LOWER(data:send:original_source_chain) AS source_chain, 
    LOWER(data:send:original_destination_chain) AS destination_chain,
    sender_address AS user, case 
      WHEN IS_ARRAY(data:send:fee_value) THEN NULL
      WHEN IS_OBJECT(data:send:fee_value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
      ELSE NULL END AS fee, 'Token Transfers' as "Service"
  FROM axelar.axelscan.fact_transfers
  WHERE status = 'executed' AND simplified_status = 'received'

  UNION ALL

  SELECT  
    created_at,
    LOWER(data:call.chain::STRING) AS source_chain,
    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user, COALESCE( CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR    IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL END, CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL END) AS fee, 'GMP' as "Service"
  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed' AND simplified_status = 'received')

SELECT date_trunc('{timeframe}',created_at) as "Date", "Service", count(distinct user) as "Number of Users", 
round(sum(fee)) as "Total Gas Fees", count(distinct (source_chain || '➡' || destination_chain)) as "Unique Paths"
FROM axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1, 2
order by 1

    """
    df = pd.read_sql(query, conn)
    return df

# === Load Data ========================================================
df_stats_overtime = load_stats_overtime(timeframe, start_date, end_date)
# === Charts: Row 4 ====================================================
color_map = {
    "Token Transfers": "#00a1f7",
    "GMP": "#ff7400"
}

col1, col2 = st.columns(2)

with col1:
    fig_stacked_fee = px.bar(df_stats_overtime, x="Date", y="Total Gas Fees", color="Service", title="Transfer Gas Fees by Service Over Time", color_discrete_map=color_map)
    fig_stacked_fee.update_layout(barmode="stack", yaxis_title="$USD", xaxis_title="", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5, title=""))
    st.plotly_chart(fig_stacked_fee, use_container_width=True)

with col2:
    fig_grouped_user = px.bar(df_stats_overtime, x="Date", y="Number of Users", color="Service", barmode="group", 
                              title="Number of Users by Service Over Time", color_discrete_map=color_map)
    fig_grouped_user.update_layout(yaxis_title="Wallet count", xaxis_title="", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5, title=""))
    st.plotly_chart(fig_grouped_user, use_container_width=True)

# --- Row 5 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# == %Transaction count ====================================================================
df_norm_tx = grouped.copy()
df_norm_tx['gmp_norm'] = df_norm_tx['gmp_num_txs'] / df_norm_tx['total_txs']
df_norm_tx['transfers_norm'] = df_norm_tx['transfers_num_txs'] / df_norm_tx['total_txs']

fig3 = go.Figure()
fig3.add_trace(go.Bar(x=df_norm_tx['period'], y=df_norm_tx['gmp_norm'], name='GMP', marker_color='#ff7400'))
fig3.add_trace(go.Bar(x=df_norm_tx['period'], y=df_norm_tx['transfers_norm'], name='Token Transfers', marker_color='#00a1f7'))
fig3.update_layout(barmode='stack', title="Normalized Transactions by Service Over Time", yaxis_tickformat='%', 
                   legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
# === %volume ==============================================================================
df_norm_vol = grouped.copy()
df_norm_vol['gmp_norm'] = df_norm_vol['gmp_volume'] / df_norm_vol['total_volume']
df_norm_vol['transfers_norm'] = df_norm_vol['transfers_volume'] / df_norm_vol['total_volume']

fig4 = go.Figure()
fig4.add_trace(go.Bar(x=df_norm_vol['period'], y=df_norm_vol['gmp_norm'], name='GMP', marker_color='#ff7400'))
fig4.add_trace(go.Bar(x=df_norm_vol['period'], y=df_norm_vol['transfers_norm'], name='Token Transfers', marker_color='#00a1f7'))
fig4.update_layout(barmode='stack', title="Normalized Volume by Service Over Time", yaxis_tickformat='%', legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
# === %User ======================================================================================
df_norm = df_stats_overtime.copy()
df_norm['total_per_date'] = df_norm.groupby("Date")["Number of Users"].transform('sum')
df_norm['normalized'] = df_norm["Number of Users"] / df_norm['total_per_date']

fig5 = go.Figure()

for service in df_norm["Service"].unique():
    df_service = df_norm[df_norm["Service"] == service]
    fig5.add_trace(go.Bar(x=df_service["Date"], y=df_service["normalized"], name=service, text=df_service["Number of Users"].astype(str),
            marker_color=color_map.get(service, None)))
fig5.update_layout(barmode='stack', title="Normalized Users by Service Over Time", xaxis_title="", yaxis_title="%", yaxis=dict(tickformat='%'), legend=dict(
        orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5, title=""))
fig5.update_traces(textposition='inside')

col1, col2, col3 = st.columns(3)

with col1:
    st.plotly_chart(fig3, use_container_width=True)

with col2:
    st.plotly_chart(fig4, use_container_width=True)

with col3:
    st.plotly_chart(fig5, use_container_width=True)

  
# --- Row 5: Donut Charts -------------------------------------------------------------------------------------------------------------------------------------------------------
total_gmp_tx = grouped['gmp_num_txs'].sum()
total_transfers_tx = grouped['transfers_num_txs'].sum()

total_gmp_vol = grouped['gmp_volume'].sum()
total_transfers_vol = grouped['transfers_volume'].sum()

tx_df = pd.DataFrame({"Service": ["GMP", "Token Transfers"], "Count": [total_gmp_tx, total_transfers_tx]})
donut_tx = px.pie(tx_df, names="Service", values="Count", color="Service", hole=0.5, title="Share of Total Transactions By Service", color_discrete_map={
        "GMP": "#ff7400",
        "Token Transfers": "#00a1f7"
    }
)

vol_df = pd.DataFrame({"Service": ["GMP", "Token Transfers"], "Volume": [total_gmp_vol, total_transfers_vol]})

donut_vol = px.pie(vol_df, names="Service", values="Volume", color="Service", hole=0.5, title="Share of Total Volume By Service", color_discrete_map={
        "GMP": "#ff7400",
        "Token Transfers": "#00a1f7"
    }
)
col5, col6 = st.columns(2)
col5.plotly_chart(donut_tx, use_container_width=True)
col6.plotly_chart(donut_vol, use_container_width=True)
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_stats_chain_fee_user_path(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
  SELECT 
    created_at, 
    LOWER(data:send:original_source_chain) AS source_chain, 
    LOWER(data:send:original_destination_chain) AS destination_chain,
    sender_address AS user, case 
      WHEN IS_ARRAY(data:send:fee_value) THEN NULL
      WHEN IS_OBJECT(data:send:fee_value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
      ELSE NULL END AS fee, 'Token Transfers' as "Service"
  FROM axelar.axelscan.fact_transfers
  WHERE status = 'executed' AND simplified_status = 'received'
  UNION ALL
  SELECT  
    created_at,
    LOWER(data:call.chain::STRING) AS source_chain,
    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user, COALESCE( CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR    IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL END, CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL END) AS fee, 'GMP' as "Service"
  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed' AND simplified_status = 'received')

SELECT "Service", count(distinct user) as "Number of Users", 
round(sum(fee)) as "Total Gas Fees", count(distinct (source_chain || '➡' || destination_chain)) as "Unique Paths"
FROM axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1
    """
    df = pd.read_sql(query, conn)
    return df

# === Load Data ===================================================================
df_stats_chain_fee_user_path = load_stats_chain_fee_user_path(start_date, end_date)
# === Charts: User, Fee, Path =====================================================
col1, col2, col3 = st.columns(3)

with col1:
    fig_stacked_fee = px.bar(df_stats_chain_fee_user_path, x="Service", y="Total Gas Fees", color="Service", title="Total Gas Fees by Service", color_discrete_map=color_map)
    fig_stacked_fee.update_layout(barmode="stack", yaxis_title="$USD", xaxis_title="")
    st.plotly_chart(fig_stacked_fee, use_container_width=True)

with col2:
    fig_stacked_user = px.bar(df_stats_chain_fee_user_path, x="Service", y="Number of Users", color="Service", title="Total Number of Users by Service", color_discrete_map=color_map)
    fig_stacked_user.update_layout(barmode="stack", yaxis_title="wallet count", xaxis_title="")
    st.plotly_chart(fig_stacked_user, use_container_width=True)

with col3:
    fig_stacked_path = px.bar(df_stats_chain_fee_user_path, x="Service", y="Unique Paths", color="Service", title="Number of Unique Paths by Service", color_discrete_map=color_map)
    fig_stacked_path.update_layout(barmode="stack", yaxis_title="Path count", xaxis_title="")
    st.plotly_chart(fig_stacked_path, use_container_width=True)
    
# --- Row 8 -------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_new_users_overtime(timeframe, start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with table1 as (WITH axelar_service AS (SELECT created_at, sender_address AS user
FROM axelar.axelscan.fact_transfers
WHERE status = 'executed' AND simplified_status = 'received'
UNION ALL
SELECT created_at, data:call.transaction.from::STRING AS user
FROM axelar.axelscan.fact_gmp 
WHERE status = 'executed' AND simplified_status = 'received')
SELECT date_trunc('{timeframe}',created_at) as "Date", count(distinct user) as "Total Users"
FROM axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1),
table2 as (with tab1 as (WITH axelar_service AS (
SELECT created_at, sender_address AS user
FROM axelar.axelscan.fact_transfers
WHERE status = 'executed' AND simplified_status = 'received'
UNION ALL
SELECT created_at, data:call.transaction.from::STRING AS user
FROM axelar.axelscan.fact_gmp 
WHERE status = 'executed' AND simplified_status = 'received')
SELECT user, min(created_at::date) as first_date
FROM axelar_service
group by 1)
select date_trunc('{timeframe}',first_date) as "Date", count(distinct user) as "New Users",
sum("New Users") over (order by "Date") as "User Growth"
from tab1
where first_date>='{start_str}' and first_date<='{end_str}'
group by 1)
select table1."Date" as "Date", "Total Users", "New Users", "Total Users"-"New Users" as "Returning Users",
"User Growth", round((("New Users"/"Total Users")*100),2) as "%New User Rate"
from table1 left join table2 on table1."Date"=table2."Date"
order by 1

    """
    df = pd.read_sql(query, conn)
    return df

# === Load Data: Row 8 ========================================================
df_new_users_overtime = load_new_users_overtime(timeframe, start_date, end_date)
# === Charts: Row 8 ============================================================
col1, col2 = st.columns(2)

with col1:
    fig_b1 = go.Figure()
    # Stacked Bars
    fig_b1.add_trace(go.Bar(x=df_new_users_overtime["Date"], y=df_new_users_overtime["New Users"], name="New Users", marker_color="#52d476"))
    fig_b1.add_trace(go.Bar(x=df_new_users_overtime["Date"], y=df_new_users_overtime["Returning Users"], name="Returning Users", marker_color="#fda569"))
    fig_b1.add_trace(go.Scatter(x=df_new_users_overtime["Date"], y=df_new_users_overtime["Total Users"], name="Total Users", mode="lines", line=dict(color="black", width=2)))
    fig_b1.update_layout(barmode="stack", title="Number of Axelar Users Over Time", yaxis=dict(title="Wallet count"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5))
    st.plotly_chart(fig_b1, use_container_width=True)

with col2:
    fig2 = px.area(df_new_users_overtime, x="Date", y="User Growth", title="Axelar Users Growth Over Time", color_discrete_sequence=["#52d476"])
    fig2.add_trace(go.Scatter(x=df_new_users_overtime["Date"], y=df_new_users_overtime["%New User Rate"], name="%New User Rate", mode="lines", yaxis="y2", line=dict(color="#ff6b05")))
    fig2.update_layout(xaxis_title="", yaxis_title="wallet count",  yaxis2=dict(title="%", overlaying="y", side="right"), template="plotly_white",
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5))
    st.plotly_chart(fig2, use_container_width=True)

# --- Tables 9, 10, 11: Command! ---------------------------------------------------------------------------------------------------------------------------------------------------
st.info("🏁 Select an Axelar service from the menu below to view its results.")
service_filter = st.selectbox("Select the Service:", options=["GMP & Token Transfers", "GMP", "Token Transfers"], index=0)

# --- Row 9: source chain analysis -------------------------------------------------------------------------------------------------------------------------------------------------
st.subheader("📤Source Chain Tracking")

@st.cache_data
def load_source_chain_tracking(start_date, end_date, service_filter):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    service_condition = ""
    if service_filter == "GMP":
        service_condition = "AND \"Service\" = 'GMP'"
    elif service_filter == "Token Transfers":
        service_condition = "AND \"Service\" = 'Token Transfers'"

    query = f"""
    WITH axelar_service AS (
  
  SELECT 
    created_at, 
    LOWER(data:send:original_source_chain) AS source_chain, 
    LOWER(data:send:original_destination_chain) AS destination_chain,
    sender_address AS user, 

    CASE 
      WHEN IS_ARRAY(data:send:amount) THEN NULL
      WHEN IS_OBJECT(data:send:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
      WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
      WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
      ELSE NULL
    END AS amount_usd,

    CASE 
      WHEN IS_ARRAY(data:send:fee_value) THEN NULL
      WHEN IS_OBJECT(data:send:fee_value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
      ELSE NULL
    END AS fee,

    id, 
    'Token Transfers' AS "Service", 
    data:link:asset::STRING AS raw_asset

  FROM axelar.axelscan.fact_transfers
  WHERE status = 'executed' AND simplified_status = 'received'
    
  UNION ALL

  SELECT  
    created_at,
    LOWER(data:call.chain::STRING) AS source_chain,
    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user,

    CASE 
      WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
      ELSE NULL
    END AS amount_usd,

    COALESCE(
      CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL
      END,
      CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL
      END
    ) AS fee,

    id, 
    'GMP' AS "Service", 
    data:symbol::STRING AS raw_asset

  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed' AND simplified_status = 'received')

SELECT source_chain as "📤Source Chain", count(distinct id) as "🚀Number of Transfers", 
count(distinct user) as "👥Number of Users", round(sum(amount_usd)) as "💸Volume of Transfers($)", 
round(sum(fee)) as "⛽Total Gas Fees($)", count(distinct destination_chain) as "📥#Destination Chains", 
count(distinct raw_asset) as "💎Number of Tokens", round(avg(fee),2) as "📊Avg Gas Fee($)", 
round(median(fee),2) as "📋Median Gas Fee"
FROM axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}' 
{service_condition}
and
id not in ('6f01df90bcb4d456c28d85a1f754f1c9c37b922885ea61f915e013aa8a20a5c6_osmosis',
'0b2b03ecd8c48bb3342754a401240fe5e421a3d74a40def8c1b77758a1976f52_osmosis',
'21074a86b299d4eaff74645ab8edc22aa3639a36e82df8e7fddfb3c78e8c7250_osmosis',
'a08cb0274fedf0594f181e6223418f1e7354c5da5285f493eeec70e4379f01bc_kujira',
'ba0ef39d7fb9b5c7650f2ea982ffb9a1f91263ce899ba1e8b13c161d0bca5e3b_secret-snip',
'efc018a03cdcfdb25f90d68fc2b06bee6c50c93c4d47ea1343148ea2444652b8_evmos',
'8e0bc8b78fd2da8b1795752fa98a4775f5dc19dca319b59ebc8a0ac80f39cfe1_osmosis',
'8eb3363bcf6776bbab9e168662173d6b24aca66f673a7f70ebebacae2d94e575_osmosis',
'71208b721ada14e26e48386396db03c7099603f452129805fa06442fb712ce85_archway',
'41e73eb192d4f9c81248c779a990f19899ae25cd3baba24f447af225430eb73e_osmosis',
'12dcc41fddd2f62e24233a3cb871689ea9d9f0c83c5b3a5ad9b629455cc7ec89_osmosis',
'562afc565b8c2e87e4018ed96cef222f80b490734fc488fdc80891a7c6f22f55_osmosis',
'606769d9cd0da39bcc93beb414c6349e3d29d3efd623e0b0829f4805438a3433_crescent',
'928031faa78c67fb1962822b3105cd359edb936751dce09e2fd807995363d3bc_osmosis',
'274969809c986ecf98013cd24b56c071df3c68b36a1c243410e866bb5b1304be_kujira',
'0xfd829bdb624a29b11a54c561d7ce80403607a79a3b4f0c6847dd4f8426274d26-121526',
'b2eb91cd813b6d107b6e3d526296d464c4e810e3ae02e0d24a1d193deb600d4b_archway',
'14115388d61f886dc1abbc2ae4cf9f68271d29605137333f9687229af671e3fc_kujira')
group by 1
order by 2 desc 

    """
    df = pd.read_sql(query, conn)
    return df

# === Load Data ======================================================================
df_source_chain_tracking = load_source_chain_tracking(start_date, end_date, service_filter)

# === Tables =========================================================================
# Criteria list
sort_options = [
    "🚀Number of Transfers",
    "👥Number of Users",
    "💸Volume of Transfers($)",
    "⛽Total Gas Fees($)",
    "📥#Destination Chains",
    "💎Number of Tokens",
    "📊Avg Gas Fee($)",
    "📋Median Gas Fee"
]
sort_by = st.selectbox("Sort by:", options=sort_options, index=0
                      )
df_display = df_source_chain_tracking.sort_values(by=sort_by, ascending=False).copy()
df_display = df_display.reset_index(drop=True)
df_display.index = df_display.index + 1
df_display = df_display.applymap(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)
st.dataframe(df_display, use_container_width=True)

# --- Row 10: destination chain analysis -------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_destination_chain_tracking(start_date, end_date, service_filter):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    service_condition = ""
    if service_filter == "GMP":
        service_condition = "AND \"Service\" = 'GMP'"
    elif service_filter == "Token Transfers":
        service_condition = "AND \"Service\" = 'Token Transfers'"

    query = f"""
    WITH axelar_service AS (
  
  SELECT 
    created_at, 
    LOWER(data:send:original_source_chain) AS source_chain, 
    LOWER(data:send:original_destination_chain) AS destination_chain,
    sender_address AS user, 

    CASE 
      WHEN IS_ARRAY(data:send:amount) THEN NULL
      WHEN IS_OBJECT(data:send:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
      WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
      WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
      ELSE NULL
    END AS amount_usd,

    CASE 
      WHEN IS_ARRAY(data:send:fee_value) THEN NULL
      WHEN IS_OBJECT(data:send:fee_value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
      ELSE NULL
    END AS fee,

    id, 
    'Token Transfers' AS "Service", 
    data:link:asset::STRING AS raw_asset

  FROM axelar.axelscan.fact_transfers
  WHERE status = 'executed' AND simplified_status = 'received'
    
  UNION ALL

  SELECT  
    created_at,
    LOWER(data:call.chain::STRING) AS source_chain,
    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user,

    CASE 
      WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
      ELSE NULL
    END AS amount_usd,

    COALESCE(
      CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL
      END,
      CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL
      END
    ) AS fee,

    id, 
    'GMP' AS "Service", 
    data:symbol::STRING AS raw_asset

  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed' AND simplified_status = 'received')

SELECT destination_chain as "📥Destination Chain", count(distinct id) as "🚀Number of Transfers", 
count(distinct user) as "👥Number of Users", round(sum(amount_usd)) as "💸Volume of Transfers($)", 
round(sum(fee)) as "⛽Total Gas Fees($)", count(distinct source_chain) as "📤#Source Chains", 
count(distinct raw_asset) as "💎Number of Tokens", round(avg(fee),2) as "📊Avg Gas Fee($)", 
round(median(fee),2) as "📋Median Gas Fee"
FROM axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}' 
{service_condition}
and
id not in ('6f01df90bcb4d456c28d85a1f754f1c9c37b922885ea61f915e013aa8a20a5c6_osmosis',
'0b2b03ecd8c48bb3342754a401240fe5e421a3d74a40def8c1b77758a1976f52_osmosis',
'21074a86b299d4eaff74645ab8edc22aa3639a36e82df8e7fddfb3c78e8c7250_osmosis',
'a08cb0274fedf0594f181e6223418f1e7354c5da5285f493eeec70e4379f01bc_kujira',
'ba0ef39d7fb9b5c7650f2ea982ffb9a1f91263ce899ba1e8b13c161d0bca5e3b_secret-snip',
'efc018a03cdcfdb25f90d68fc2b06bee6c50c93c4d47ea1343148ea2444652b8_evmos',
'8e0bc8b78fd2da8b1795752fa98a4775f5dc19dca319b59ebc8a0ac80f39cfe1_osmosis',
'8eb3363bcf6776bbab9e168662173d6b24aca66f673a7f70ebebacae2d94e575_osmosis',
'71208b721ada14e26e48386396db03c7099603f452129805fa06442fb712ce85_archway',
'41e73eb192d4f9c81248c779a990f19899ae25cd3baba24f447af225430eb73e_osmosis',
'12dcc41fddd2f62e24233a3cb871689ea9d9f0c83c5b3a5ad9b629455cc7ec89_osmosis',
'562afc565b8c2e87e4018ed96cef222f80b490734fc488fdc80891a7c6f22f55_osmosis',
'606769d9cd0da39bcc93beb414c6349e3d29d3efd623e0b0829f4805438a3433_crescent',
'928031faa78c67fb1962822b3105cd359edb936751dce09e2fd807995363d3bc_osmosis',
'274969809c986ecf98013cd24b56c071df3c68b36a1c243410e866bb5b1304be_kujira',
'0xfd829bdb624a29b11a54c561d7ce80403607a79a3b4f0c6847dd4f8426274d26-121526',
'b2eb91cd813b6d107b6e3d526296d464c4e810e3ae02e0d24a1d193deb600d4b_archway',
'14115388d61f886dc1abbc2ae4cf9f68271d29605137333f9687229af671e3fc_kujira')
group by 1
order by 2 desc 

    """
    df = pd.read_sql(query, conn)
    return df

# === Load Data ======================================================================
df_destination_chain_tracking = load_destination_chain_tracking(start_date, end_date, service_filter)

# === Tables =========================================================================
st.subheader("📥Destination Chain Tracking")
# Criteria list
sort_options = [
    "🚀Number of Transfers",
    "👥Number of Users",
    "💸Volume of Transfers($)",
    "⛽Total Gas Fees($)",
    "📤#Source Chains",
    "💎Number of Tokens",
    "📊Avg Gas Fee($)",
    "📋Median Gas Fee"
]
sort_by = st.selectbox("Sort by:", options=sort_options, index=0
                      )
df_display = df_destination_chain_tracking.sort_values(by=sort_by, ascending=False).copy()
df_display = df_display.reset_index(drop=True)
df_display.index = df_display.index + 1
df_display = df_display.applymap(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)
st.dataframe(df_display, use_container_width=True)

# --- Row 11: paths analysis ------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_path_tracking(start_date, end_date, service_filter):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    service_condition = ""
    if service_filter == "GMP":
        service_condition = "AND \"Service\" = 'GMP'"
    elif service_filter == "Token Transfers":
        service_condition = "AND \"Service\" = 'Token Transfers'"

    query = f"""
    WITH axelar_service AS (
  
  SELECT 
    created_at, 
    LOWER(data:send:original_source_chain) AS source_chain, 
    LOWER(data:send:original_destination_chain) AS destination_chain,
    sender_address AS user, 

    CASE 
      WHEN IS_ARRAY(data:send:amount) THEN NULL
      WHEN IS_OBJECT(data:send:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
      WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
      WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
      ELSE NULL
    END AS amount_usd,

    CASE 
      WHEN IS_ARRAY(data:send:fee_value) THEN NULL
      WHEN IS_OBJECT(data:send:fee_value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
      ELSE NULL
    END AS fee,

    id, 
    'Token Transfers' AS "Service", 
    data:link:asset::STRING AS raw_asset

  FROM axelar.axelscan.fact_transfers
  WHERE status = 'executed' AND simplified_status = 'received'
    
  UNION ALL

  SELECT  
    created_at,
    LOWER(data:call.chain::STRING) AS source_chain,
    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user,

    CASE 
      WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
      ELSE NULL
    END AS amount_usd,

    COALESCE(
      CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL
      END,
      CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL
      END
    ) AS fee,

    id, 
    'GMP' AS "Service", 
    data:symbol::STRING AS raw_asset

  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed' AND simplified_status = 'received')

SELECT source_chain || '➡' || destination_chain as "🎯Path", count(distinct id) as "🚀Number of Transfers", 
count(distinct user) as "👥Number of Users", round(sum(amount_usd)) as "💸Volume of Transfers($)", 
round(sum(fee)) as "⛽Total Gas Fees($)", 
count(distinct raw_asset) as "💎Number of Tokens", round(avg(fee),2) as "📊Avg Gas Fee($)", 
round(median(fee),2) as "📋Median Gas Fee"
FROM axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}' 
{service_condition}
and
id not in ('6f01df90bcb4d456c28d85a1f754f1c9c37b922885ea61f915e013aa8a20a5c6_osmosis',
'0b2b03ecd8c48bb3342754a401240fe5e421a3d74a40def8c1b77758a1976f52_osmosis',
'21074a86b299d4eaff74645ab8edc22aa3639a36e82df8e7fddfb3c78e8c7250_osmosis',
'a08cb0274fedf0594f181e6223418f1e7354c5da5285f493eeec70e4379f01bc_kujira',
'ba0ef39d7fb9b5c7650f2ea982ffb9a1f91263ce899ba1e8b13c161d0bca5e3b_secret-snip',
'efc018a03cdcfdb25f90d68fc2b06bee6c50c93c4d47ea1343148ea2444652b8_evmos',
'8e0bc8b78fd2da8b1795752fa98a4775f5dc19dca319b59ebc8a0ac80f39cfe1_osmosis',
'8eb3363bcf6776bbab9e168662173d6b24aca66f673a7f70ebebacae2d94e575_osmosis',
'71208b721ada14e26e48386396db03c7099603f452129805fa06442fb712ce85_archway',
'41e73eb192d4f9c81248c779a990f19899ae25cd3baba24f447af225430eb73e_osmosis',
'12dcc41fddd2f62e24233a3cb871689ea9d9f0c83c5b3a5ad9b629455cc7ec89_osmosis',
'562afc565b8c2e87e4018ed96cef222f80b490734fc488fdc80891a7c6f22f55_osmosis',
'606769d9cd0da39bcc93beb414c6349e3d29d3efd623e0b0829f4805438a3433_crescent',
'928031faa78c67fb1962822b3105cd359edb936751dce09e2fd807995363d3bc_osmosis',
'274969809c986ecf98013cd24b56c071df3c68b36a1c243410e866bb5b1304be_kujira',
'0xfd829bdb624a29b11a54c561d7ce80403607a79a3b4f0c6847dd4f8426274d26-121526',
'b2eb91cd813b6d107b6e3d526296d464c4e810e3ae02e0d24a1d193deb600d4b_archway',
'14115388d61f886dc1abbc2ae4cf9f68271d29605137333f9687229af671e3fc_kujira')
group by 1
order by 2 desc 

    """
    df = pd.read_sql(query, conn)
    return df

# === Load Data ======================================================================
df_path_tracking = load_path_tracking(start_date, end_date, service_filter)

# === Tables =========================================================================
st.subheader("🎯Path Tracking")
# Criteria list
sort_options = [
    "🚀Number of Transfers",
    "👥Number of Users",
    "💸Volume of Transfers($)",
    "⛽Total Gas Fees($)",
    "💎Number of Tokens",
    "📊Avg Gas Fee($)",
    "📋Median Gas Fee"
]
sort_by = st.selectbox("Sort by:", options=sort_options, index=0
                      )
df_display = df_path_tracking.sort_values(by=sort_by, ascending=False).copy()
df_display = df_display.reset_index(drop=True)
df_display.index = df_display.index + 1
df_display = df_display.applymap(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)
st.dataframe(df_display, use_container_width=True)
