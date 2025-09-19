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

st.markdown(
    """
    <div style="background-color:#ff7f27; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Axelar Cross-Chain Transfers Overview</h2>
    </div>
    """,
    unsafe_allow_html=True
)

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
                              title="Number of Active Users by Service Over Time", color_discrete_map=color_map)
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
fig3.update_layout(barmode='stack', title="Normalized Transactions Over Time", yaxis_tickformat='%',
                   legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
# === %volume ==============================================================================
df_norm_vol = grouped.copy()
df_norm_vol['gmp_norm'] = df_norm_vol['gmp_volume'] / df_norm_vol['total_volume']
df_norm_vol['transfers_norm'] = df_norm_vol['transfers_volume'] / df_norm_vol['total_volume']

fig4 = go.Figure()
fig4.add_trace(go.Bar(x=df_norm_vol['period'], y=df_norm_vol['gmp_norm'], name='GMP', marker_color='#ff7400'))
fig4.add_trace(go.Bar(x=df_norm_vol['period'], y=df_norm_vol['transfers_norm'], name='Token Transfers', marker_color='#00a1f7'))
fig4.update_layout(barmode='stack', title="Normalized Volume Over Time", yaxis_tickformat='%', 
                   legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
# === %User ======================================================================================
df_norm = df_stats_overtime.copy()
df_norm['total_per_date'] = df_norm.groupby("Date")["Number of Users"].transform('sum')
df_norm['normalized'] = df_norm["Number of Users"] / df_norm['total_per_date']

fig5 = go.Figure()

for service in df_norm["Service"].unique():
    df_service = df_norm[df_norm["Service"] == service]
    fig5.add_trace(go.Bar(x=df_service["Date"], y=df_service["normalized"], name=service, text=df_service["Number of Users"].astype(str),
            marker_color=color_map.get(service, None)))
fig5.update_layout(barmode='stack', title="Normalized Users Over Time", xaxis_title="", yaxis=dict(tickformat='%'), legend=dict(
        orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5, title=""))
fig5.update_traces(textposition='inside')

col1, col2, col3 = st.columns(3)

with col1:
    st.plotly_chart(fig3, use_container_width=True)

with col2:
    st.plotly_chart(fig4, use_container_width=True)

with col3:
    st.plotly_chart(fig5, use_container_width=True)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------    
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
  
# --- Row 6: Donut Charts -------------------------------------------------------------------------------------------------------------------------------------------------------
total_gmp_tx = grouped['gmp_num_txs'].sum()
total_transfers_tx = grouped['transfers_num_txs'].sum()

total_gmp_vol = grouped['gmp_volume'].sum()
total_transfers_vol = grouped['transfers_volume'].sum()

tx_df = pd.DataFrame({"Service": ["GMP", "Token Transfers"], "Count": [total_gmp_tx, total_transfers_tx]})
donut_tx = px.pie(tx_df, names="Service", values="Count", color="Service", hole=0.5, title="Total Transactions by Service", color_discrete_map={
        "GMP": "#ff7400",
        "Token Transfers": "#00a1f7"
    }
)
donut_tx.update_traces(textinfo='label+percent', showlegend=False)

vol_df = pd.DataFrame({"Service": ["GMP", "Token Transfers"], "Volume": [total_gmp_vol, total_transfers_vol]})
donut_vol = px.pie(vol_df, names="Service", values="Volume", color="Service", hole=0.5, title="Total Volume by Service", color_discrete_map={
        "GMP": "#ff7400",
        "Token Transfers": "#00a1f7"
    }
)
donut_vol.update_traces(textinfo='label+percent', showlegend=False)

# ------------------------
fig_stacked_fee = px.bar(df_stats_chain_fee_user_path, x="Service", y="Total Gas Fees", color="Service", title="Total Gas Fees by Service", color_discrete_map=color_map)
fig_stacked_fee.update_layout(barmode="stack", yaxis_title="$USD", xaxis_title="")
fig_stacked_fee.update_traces(text=df_stats_chain_fee_user_path["Total Gas Fees"], texttemplate='%{text}', textposition='inside', showlegend=False) 
# ------------------------
fig_stacked_user = px.bar(df_stats_chain_fee_user_path, x="Service", y="Number of Users", color="Service", title="Total Number of Users by Service", color_discrete_map=color_map)
fig_stacked_user.update_layout(barmode="stack", yaxis_title="wallet count", xaxis_title="")
fig_stacked_user.update_traces(text=df_stats_chain_fee_user_path["Number of Users"], texttemplate='%{text}', textposition='inside', showlegend=False)    

col5, col6, col7, col8 = st.columns(4)
col5.plotly_chart(donut_tx, use_container_width=True)
col6.plotly_chart(donut_vol, use_container_width=True)
col7.plotly_chart(fig_stacked_fee, use_container_width=True)
col8.plotly_chart(fig_stacked_user, use_container_width=True)   

