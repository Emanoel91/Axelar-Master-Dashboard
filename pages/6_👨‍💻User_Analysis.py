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
st.title("👨‍💻User Analysis")

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
        <h2 style="color:#000000; text-align:center;">Analysis of Axelar Users</h2>
    </div>
    """,
    unsafe_allow_html=True
)
st.markdown("<br>", unsafe_allow_html=True)
# --- Row 1 ------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_user_stats(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
  
  SELECT 
    created_at, 
    LOWER(data:send:original_source_chain) AS source_chain, 
    LOWER(data:send:original_destination_chain) AS destination_chain,
    sender_address AS user, case 
    WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
    WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
    WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
    THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
    ELSE NULL END AS amount_usd, CASE 
    WHEN IS_ARRAY(data:send:fee_value) THEN NULL
    WHEN IS_OBJECT(data:send:fee_value) THEN NULL
    WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
    ELSE NULL END AS fee, id
    FROM axelar.axelscan.fact_transfers
    WHERE status = 'executed' AND simplified_status = 'received' 
    UNION ALL
    SELECT created_at, LOWER(data:call.chain::STRING) AS source_chain, LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user, CASE 
    WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
    WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
    ELSE NULL
    END AS amount_usd, COALESCE(CASE 
    WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
    OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
    THEN NULL
    WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
    AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
    THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
    ELSE NULL END, CASE 
    WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
    WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
    ELSE NULL END) AS fee, id
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed' AND simplified_status = 'received')
    
    SELECT count(distinct user) as "Number of Users", round(sum(amount_usd)/count(distinct user)) as "Avg Volume per User", 
    round(count(distinct id)/count(distinct user)) as "Avg Txns per User"
    FROM axelar_service
    where created_at::date>='{start_str}' and created_at::date<='{end_str}' and
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
    """
    df = pd.read_sql(query, conn)
    return df

# === Load Data =====================================
df_user_stats = load_user_stats(start_date, end_date)
# === KPIs: Row 1 ===================================
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
    st.markdown(card_style.format(label="Unique Users", value=f"{df_user_stats["Number of Users"][0]:,} Wallets"), unsafe_allow_html=True)
with col2:
    st.markdown(card_style.format(label="Avg Volume per User", value=f"${df_user_stats["Avg Volume per User"][0]:,}"), unsafe_allow_html=True)
with col3:
    st.markdown(card_style.format(label="Avg Txns per User", value=f"{df_user_stats["Avg Txns per User"][0]:,} Txns"), unsafe_allow_html=True)

# --- Row 2 -------------------------------------------------------------------------------------------------------------------------------------------------------------------
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

# === Load Data: Row 2 ========================================================
df_new_users_overtime = load_new_users_overtime(timeframe, start_date, end_date)
# === Charts: Row 2 ============================================================
col1, col2 = st.columns(2)

with col1:
    fig_b1 = go.Figure()
    # Stacked Bars
    fig_b1.add_trace(go.Bar(x=df_new_users_overtime["Date"], y=df_new_users_overtime["New Users"], name="New Users", marker_color="#52d476"))
    fig_b1.add_trace(go.Bar(x=df_new_users_overtime["Date"], y=df_new_users_overtime["Returning Users"], name="Returning Users", marker_color="#ffcf68"))
    fig_b1.add_trace(go.Scatter(x=df_new_users_overtime["Date"], y=df_new_users_overtime["Total Users"], name="Total Users", mode="lines", line=dict(color="#00a8f3", width=2)))
    fig_b1.update_layout(barmode="stack", title="Number of Axelar Users Over Time", yaxis=dict(title="Wallet count"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5))
    st.plotly_chart(fig_b1, use_container_width=True)

with col2:
    fig2 = px.area(df_new_users_overtime, x="Date", y="User Growth", title="Axelar Users Growth Over Time", color_discrete_sequence=["#52d476"])
    fig2.add_trace(go.Scatter(x=df_new_users_overtime["Date"], y=df_new_users_overtime["%New User Rate"], name="%New User Rate", mode="lines", yaxis="y2", line=dict(color="#00a8f3")))
    fig2.update_layout(xaxis_title="", yaxis_title="wallet count",  yaxis2=dict(title="%", overlaying="y", side="right"), template="plotly_white",
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5))
    st.plotly_chart(fig2, use_container_width=True)
# --- Row 3 ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="background-color:#ff7f27; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Distribution of Axelar Users</h2>
    </div>
    """,
    unsafe_allow_html=True
)
st.markdown("<br>", unsafe_allow_html=True)

# --- Info Box ---
st.markdown(
    """
    <div style="background-color: #a3fcbc; padding: 15px; border-radius: 10px; border: 1px solid #a3fcbc;">
        <strong>🔸Distribution of Users by Transfer Volume:</strong> Categorizes users based on their <strong>total transfer volume</strong>.<br><br>
        <strong>🔸Distribution of Transfers by Transaction Size:</strong> Categorizes transactions based on their <strong>individual transfer size</strong>.<br><br>
        <strong>🔸Distribution of Users by Number of Transfers:</strong> Groups users according to the <strong>total number of transfers</strong> they have conducted.<br><br>
        <strong>🔸User Activity by Number of Cross-Chain Routes:</strong> Classifies users based on the <strong>number of cross-chain routes</strong> they utilize for 
        transferring assets.<br><br><strong>🔸Distribution of Users by Unique Active Days/Weeks/Months:</strong> Measures user activity by 
        the <strong>number of unique days/weeks/months</strong> they 
        engaged with Axelar interchain services.<br><br>
    </div>
    """,
    unsafe_allow_html=True
)
# --- Row 3 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_distribution_txn_size(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with overview as (
    WITH axelar_service AS (
    SELECT 
    created_at, 
    LOWER(data:send:original_source_chain) AS source_chain, 
    LOWER(data:send:original_destination_chain) AS destination_chain,
    sender_address AS user, case 
    WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
    WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
    WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
    THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
    ELSE NULL END AS amount_usd, CASE 
    WHEN IS_ARRAY(data:send:fee_value) THEN NULL
    WHEN IS_OBJECT(data:send:fee_value) THEN NULL
    WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
    ELSE NULL END AS fee, id
    FROM axelar.axelscan.fact_transfers
    WHERE status = 'executed' AND simplified_status = 'received' 
    UNION ALL
    SELECT created_at, LOWER(data:call.chain::STRING) AS source_chain, LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user, CASE 
    WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
    WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
    ELSE NULL
    END AS amount_usd, COALESCE(CASE 
    WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
    OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
    THEN NULL
    WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
    AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
    THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
    ELSE NULL END, CASE 
    WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
    WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
    ELSE NULL END) AS fee, id
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed' AND simplified_status = 'received')
    
    SELECT id, case 
    when amount_usd<=1 then 'V<=1$'
    when amount_usd>1 and amount_usd<=10 then '1<V<=10$'
    when amount_usd>10 and amount_usd<=100 then '10<V<=100$'
    when amount_usd>100 and amount_usd<=1000 then '100<V<=1k$'
    when amount_usd>1000 and amount_usd<=10000 then '1k<V<=10k$'
    when amount_usd>10000 and amount_usd<=100000 then '10k<V<=100k$'
    when amount_usd>100000 then 'V>100k$'
    else 'No Volume'
    end as "Class"
    FROM axelar_service
    where created_at::date>='{start_str}' and created_at::date<='{end_str}' and
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
    '14115388d61f886dc1abbc2ae4cf9f68271d29605137333f9687229af671e3fc_kujira'))
    select "Class", count(distinct id) as "Number of Transfers"
    from overview 
    group by 1
    order by 2 desc 
    """
    df = pd.read_sql(query, conn)
    return df
# =======================================
@st.cache_data
def load_distribution_user_size(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with overview as (
    WITH axelar_service AS (
    SELECT 
    created_at, 
    LOWER(data:send:original_source_chain) AS source_chain, 
    LOWER(data:send:original_destination_chain) AS destination_chain,
    sender_address AS user, case 
    WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
    WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
    WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
    THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
    ELSE NULL END AS amount_usd, CASE 
    WHEN IS_ARRAY(data:send:fee_value) THEN NULL
    WHEN IS_OBJECT(data:send:fee_value) THEN NULL
    WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
    ELSE NULL END AS fee, id
    FROM axelar.axelscan.fact_transfers
    WHERE status = 'executed' AND simplified_status = 'received' 
    UNION ALL
    SELECT created_at, LOWER(data:call.chain::STRING) AS source_chain, LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user, CASE 
    WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
    WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
    ELSE NULL
    END AS amount_usd, COALESCE(CASE 
    WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
    OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
    THEN NULL
    WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
    AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
    THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
    ELSE NULL END, CASE 
    WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
    WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
    ELSE NULL END) AS fee, id
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed' AND simplified_status = 'received')
    
    SELECT user, sum(amount_usd), case 
    when sum(amount_usd)<=1 then 'V<=1$'
    when sum(amount_usd)>1 and sum(amount_usd)<=10 then '1<V<=10$'
    when sum(amount_usd)>10 and sum(amount_usd)<=100 then '10<V<=100$'
    when sum(amount_usd)>100 and sum(amount_usd)<=1000 then '100<V<=1k$'
    when sum(amount_usd)>1000 and sum(amount_usd)<=10000 then '1k<V<=10k$'
    when sum(amount_usd)>10000 and sum(amount_usd)<=100000 then '10k<V<=100k$'
    when sum(amount_usd)>100000 and sum(amount_usd)<=1000000 then '100k<V<=1m$'
    when sum(amount_usd)>1000000 then 'V>1m$'
    else 'No Volume'
    end as "Class"
    FROM axelar_service
    where created_at::date>='{start_str}' and created_at::date<='{end_str}' and 
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
    group by 1)
    select "Class", count(distinct user) as "Number of Users"
    from overview 
    group by 1
    order by 2 desc 
    """
    df = pd.read_sql(query, conn)
    return df
# === Load Data: Row 3 ============================================================
df_distribution_txn_size = load_distribution_txn_size(start_date, end_date)
df_distribution_user_size = load_distribution_user_size(start_date, end_date)    
# === Charts: Row 3 ===============================================================
color_scale = {
    'V<=1$': '#bdfde8',       
    '1<V<=10$': '#9dfcdc',
    '10<V<=100$': '#6ffccd',
    '100<V<=1k$': '#3afebc',
    '1k<V<=10k$': '#0dffae',
    '10k<V<=100k$': '#06d792',
    'V>100k$': '#01b378',
    'No Volume': '#ffcf68'
}

fig_donut_txn_volume = px.pie(df_distribution_txn_size, names="Class", values="Number of Transfers", title="Distribution of Transfers By Transaction Size", hole=0.5, color="Class",
    color_discrete_map=color_scale)
fig_donut_txn_volume.update_traces(textposition='inside', textinfo='percent+label', pull=[0.05]*len(df_distribution_txn_size))
fig_donut_txn_volume.update_layout(showlegend=True, legend=dict(orientation="v", y=0.5, x=1.1))

# ---------------------------------------
color_scale = {
    'V<=1$': '#bdfde8',       
    '1<V<=10$': '#9dfcdc',
    '10<V<=100$': '#6ffccd',
    '100<V<=1k$': '#3afebc',
    '1k<V<=10k$': '#0dffae',
    '10k<V<=100k$': '#06d792',
    '100k<V<=1m$': '#01b378',
    'V>1m$': '#faad29',
    'No Volume': '#ffcf68'
}

fig_donut_user_size = px.pie(df_distribution_user_size, names="Class", values="Number of Users", title="Distribution of Users By Transfers Volume", hole=0.5, 
                       color="Class", color_discrete_map=color_scale)
fig_donut_user_size.update_traces(textposition='inside', textinfo='percent+label', pull=[0.05]*len(df_distribution_user_size))
fig_donut_user_size.update_layout(showlegend=True, legend=dict(orientation="v", y=0.5, x=1.1))

col1, col2 = st.columns(2)

with col1:
    st.plotly_chart(fig_donut_txn_volume, use_container_width=True)

with col2:
    st.plotly_chart(fig_donut_user_size, use_container_width=True)

# --- Row 4 --------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_distribution_user_txncount(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with overview as (
    WITH axelar_service AS (
    SELECT 
    created_at, 
    LOWER(data:send:original_source_chain) AS source_chain, 
    LOWER(data:send:original_destination_chain) AS destination_chain,
    sender_address AS user, case 
    WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
    WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
    WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
    THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
    ELSE NULL END AS amount_usd, CASE 
    WHEN IS_ARRAY(data:send:fee_value) THEN NULL
    WHEN IS_OBJECT(data:send:fee_value) THEN NULL
    WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
    ELSE NULL END AS fee, id
    FROM axelar.axelscan.fact_transfers
    WHERE status = 'executed' AND simplified_status = 'received' 
    UNION ALL
    SELECT created_at, LOWER(data:call.chain::STRING) AS source_chain, LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user, CASE 
    WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
    WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
    ELSE NULL
    END AS amount_usd, COALESCE(CASE 
    WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
    OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
    THEN NULL
    WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
    AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
    THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
    ELSE NULL END, CASE 
    WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
    WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
    ELSE NULL END) AS fee, id
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed' AND simplified_status = 'received')
    
    SELECT user, count(distinct id), case 
    when count(distinct id)=1 then '1 Txn'
    when count(distinct id)>1 and count(distinct id)<=5 then '2-5 Txns'
    when count(distinct id)>5 and count(distinct id)<=10 then '6-10 Txns'
    when count(distinct id)>10 and count(distinct id)<=20 then '11-20 Txns'
    when count(distinct id)>20 and count(distinct id)<=50 then '21-50 Txns'
    when count(distinct id)>50 and count(distinct id)<=100 then '51-100 Txns'
    when count(distinct id)>100 and count(distinct id)<=200 then '101-200 Txns'
    when count(distinct id)>200 and count(distinct id)<=500 then '201-500 Txns'
    when count(distinct id)>500 and count(distinct id)<=1000 then '501-1000 Txns'
    when count(distinct id)>1000 then '>1000 Txns'
    end as "Class"
    FROM axelar_service
    where created_at::date>='{start_str}' and created_at::date<='{end_str}' and
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
    group by 1)
    select "Class", count(distinct user) as "Number of Users"
    from overview 
    group by 1
    order by 2 desc 
    """
    df = pd.read_sql(query, conn)
    return df

# ====================================
@st.cache_data
def load_distribution_user_route(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with overview as (
    WITH axelar_service AS (
    SELECT 
    created_at, 
    LOWER(data:send:original_source_chain) AS source_chain, 
    LOWER(data:send:original_destination_chain) AS destination_chain,
    sender_address AS user, case 
    WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
    WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
    WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
    THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
    ELSE NULL END AS amount_usd, CASE 
    WHEN IS_ARRAY(data:send:fee_value) THEN NULL
    WHEN IS_OBJECT(data:send:fee_value) THEN NULL
    WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
    ELSE NULL END AS fee, id
    FROM axelar.axelscan.fact_transfers
    WHERE status = 'executed' AND simplified_status = 'received' 
    UNION ALL
    SELECT created_at, LOWER(data:call.chain::STRING) AS source_chain, LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user, CASE 
    WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
    WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
    ELSE NULL
    END AS amount_usd, COALESCE(CASE 
    WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
    OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
    THEN NULL
    WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
    AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
    THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
    ELSE NULL END, CASE 
    WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
    WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
    ELSE NULL END) AS fee, id
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed' AND simplified_status = 'received')
    
    SELECT user, count(distinct (source_chain || '➡' || destination_chain)), case 
            when count(distinct (source_chain || '➡' || destination_chain)) = 1 then 'Single Route Users (n=1)'
            when (count(distinct (source_chain || '➡' || destination_chain)) = 2 or count(distinct (source_chain || '➡' || destination_chain)) = 3) then 'Multi-Route Explorers (n=2,3)'
            when (count(distinct (source_chain || '➡' || destination_chain)) = 4 or count(distinct (source_chain || '➡' || destination_chain)) = 5) then 'Network Navigators (n=4,5)'
            when (count(distinct (source_chain || '➡' || destination_chain)) >= 6 and count(distinct (source_chain || '➡' || destination_chain)) <= 10) then 'Bridge Veterans (n=6-10)'
            when (count(distinct (source_chain || '➡' || destination_chain)) > 10) then 'Cross-Chain Masters (n>10)'
    end as "Class"
    FROM axelar_service
    where created_at::date>='{start_str}' and created_at::date<='{end_str}' and source_chain is not null and destination_chain is not null and 
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
    group by 1)
    select "Class", count(distinct user) as "Number of Users"
    from overview 
    group by 1
    order by 2 desc 
    """
    df = pd.read_sql(query, conn)
    return df

# === Load Data: Row 4 ==============================================================
df_distribution_user_txncount = load_distribution_user_txncount(start_date, end_date)
df__distribution_user_route = load_distribution_user_route(start_date, end_date)
# === Charts: Row 4 =================================================================
bar_fig = px.bar(df_distribution_user_txncount, x="Class", y="Number of Users", title="Distribution of Users By Number of Transfers", color_discrete_sequence=["#00da98"])
bar_fig.update_layout(xaxis_title=" ", yaxis_title="Wallet count", bargap=0.2)

# =========================
color_scale = {
    'Single Route Users (n=1)': '#bdfde8',       
    'Multi-Route Explorers (n=2,3)': '#9dfcdc',
    'Network Navigators (n=4,5)': '#6ffccd',
    'Bridge Veterans (n=6-10)': '#3afebc',
    'Cross-Chain Masters (n>10)': '#0dffae'
}
fig_donut_route = px.pie(df__distribution_user_route, names="Class", values="Number of Users", title="User Activity: Grouped by Number of Cross-Chain Routes", 
                         hole=0.5, color="Class", color_discrete_map=color_scale)
fig_donut_route.update_traces(textposition='inside', textinfo='percent+label', pull=[0.05]*len(df__distribution_user_route))
fig_donut_route.update_layout(showlegend=True, legend=dict(orientation="v", y=0.5, x=1.1))

col1, col2 = st.columns(2)

with col1:
    st.plotly_chart(bar_fig, use_container_width=True)

with col2:
    st.plotly_chart(fig_donut_route, use_container_width=True)

# --- Row 5 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_user_day(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with overview as (
    WITH axelar_service AS (
    SELECT 
    created_at, 
    LOWER(data:send:original_source_chain) AS source_chain, 
    LOWER(data:send:original_destination_chain) AS destination_chain,
    sender_address AS user, case 
    WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
    WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
    WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
    THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
    ELSE NULL END AS amount_usd, CASE 
    WHEN IS_ARRAY(data:send:fee_value) THEN NULL
    WHEN IS_OBJECT(data:send:fee_value) THEN NULL
    WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
    ELSE NULL END AS fee, id
    FROM axelar.axelscan.fact_transfers
    WHERE status = 'executed' AND simplified_status = 'received' 
    UNION ALL
    SELECT created_at, LOWER(data:call.chain::STRING) AS source_chain, LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user, CASE 
    WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
    WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
    ELSE NULL
    END AS amount_usd, COALESCE(CASE 
    WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
    OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
    THEN NULL
    WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
    AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
    THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
    ELSE NULL END, CASE 
    WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
    WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
    ELSE NULL END) AS fee, id
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed' AND simplified_status = 'received')
    
    SELECT user, count(distinct created_at::date) as "Active Days"
    FROM axelar_service
    where created_at::date>='{start_str}' and created_at::date<='{end_str}' and
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
    group by 1)
    select "Active Days", count(distinct user) as "Number of Users"
    from overview 
    group by 1
    order by 2 desc 
    """
    df = pd.read_sql(query, conn)
    return df

# === Load Data: Row 5 =======================================
df_user_day = load_user_day(start_date, end_date)
# === Chart: Row 5 ===========================================
fig_bulb = px.bar(df_user_day, x="Active Days", y="Number of Users", color="Active Days", title="Distribution of Users According to the Number of Days They Were Active")
fig_bulb.update_layout(yaxis=dict(title="Number of Users", type="log"), xaxis=dict(title="Number of Days of Activity"))
st.plotly_chart(fig_bulb, use_container_width=True)

# --- Row 6 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_user_week(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with overview as (
    WITH axelar_service AS (
    SELECT 
    created_at, 
    LOWER(data:send:original_source_chain) AS source_chain, 
    LOWER(data:send:original_destination_chain) AS destination_chain,
    sender_address AS user, case 
    WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
    WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
    WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
    THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
    ELSE NULL END AS amount_usd, CASE 
    WHEN IS_ARRAY(data:send:fee_value) THEN NULL
    WHEN IS_OBJECT(data:send:fee_value) THEN NULL
    WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
    ELSE NULL END AS fee, id
    FROM axelar.axelscan.fact_transfers
    WHERE status = 'executed' AND simplified_status = 'received' 
    UNION ALL
    SELECT created_at, LOWER(data:call.chain::STRING) AS source_chain, LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user, CASE 
    WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
    WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
    ELSE NULL
    END AS amount_usd, COALESCE(CASE 
    WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
    OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
    THEN NULL
    WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
    AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
    THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
    ELSE NULL END, CASE 
    WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
    WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
    ELSE NULL END) AS fee, id
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed' AND simplified_status = 'received')
    
    SELECT user, count(distinct date_trunc('week',created_at)) as "Active Weeks"
    FROM axelar_service
    where created_at::date>='{start_str}' and created_at::date<='{end_str}' and
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
    group by 1)
    select "Active Weeks", count(distinct user) as "Number of Users"
    from overview 
    group by 1
    order by 2 desc 
    """
    df = pd.read_sql(query, conn)
    return df

# === Load Data: Row 6 =======================================
df_user_week = load_user_week(start_date, end_date)
# === Chart: Row 6 ===========================================
fig_bulb = px.bar(df_user_week, x="Active Weeks", y="Number of Users", color="Active Weeks", title="Distribution of Users According to the Number of Weeks They Were Active")
fig_bulb.update_layout(yaxis=dict(title="Number of Users", type="log"), xaxis=dict(title="Number of Weeks of Activity"))
st.plotly_chart(fig_bulb, use_container_width=True)
# --- Row 7 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_user_month(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with overview as (
    WITH axelar_service AS (
    SELECT 
    created_at, 
    LOWER(data:send:original_source_chain) AS source_chain, 
    LOWER(data:send:original_destination_chain) AS destination_chain,
    sender_address AS user, case 
    WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
    WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
    WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
    THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
    ELSE NULL END AS amount_usd, CASE 
    WHEN IS_ARRAY(data:send:fee_value) THEN NULL
    WHEN IS_OBJECT(data:send:fee_value) THEN NULL
    WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
    ELSE NULL END AS fee, id
    FROM axelar.axelscan.fact_transfers
    WHERE status = 'executed' AND simplified_status = 'received' 
    UNION ALL
    SELECT created_at, LOWER(data:call.chain::STRING) AS source_chain, LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user, CASE 
    WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
    WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
    ELSE NULL
    END AS amount_usd, COALESCE(CASE 
    WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
    OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
    THEN NULL
    WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
    AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
    THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
    ELSE NULL END, CASE 
    WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
    WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
    ELSE NULL END) AS fee, id
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed' AND simplified_status = 'received')
    
    SELECT user, count(distinct date_trunc('month',created_at)) as "Active Months"
    FROM axelar_service
    where created_at::date>='{start_str}' and created_at::date<='{end_str}' and
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
    group by 1)
    select "Active Months", count(distinct user) as "Number of Users"
    from overview 
    group by 1
    order by 2 desc 
    """
    df = pd.read_sql(query, conn)
    return df

# === Load Data: Row 7 =======================================
df_user_month = load_user_month(start_date, end_date)
# === Chart: Row 7 ===========================================
fig_bulb = px.bar(df_user_month, x="Active Months", y="Number of Users", color="Active Months", title="Distribution of Users According to the Number of Months They Were Active")
fig_bulb.update_layout(yaxis=dict(title="Number of Users", type="log"), xaxis=dict(title="Number of Months of Activity"))
st.plotly_chart(fig_bulb, use_container_width=True)

# --- Row 8 ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="background-color:#ff7f27; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Axelar User Retention</h2>
    </div>
    """,
    unsafe_allow_html=True
)
st.markdown("<br>", unsafe_allow_html=True)

@st.cache_data
def load_its_user_retention():

    query = f"""
    with base as (SELECT  
    data:call.transaction.from::STRING AS tx_signer,
    min(date_trunc('month', created_at)) over (partition by tx_signer) as signup_date,
    date_trunc('month', created_at) as activity_date,
    datediff('month', signup_date, activity_date) as difference
  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed'
    AND simplified_status = 'received'
    AND (data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%'
        or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%')),
unp as (
  select TO_VARCHAR(signup_date, 'yyyy-MM') as cohort_date, difference as months, count (distinct TX_SIGNER) as users
  from base
  where datediff('month', signup_date, current_date()) <= 24
  group by 1,2
  order by 1),
fine as (select u.*, p.USERS as user0
  from unp u left join unp p on u.COHORT_DATE = p.COHORT_DATE
  where p.MONTHS = 0)
select 
  COHORT_DATE as "Cohort Date", 
  MONTHS as "Month",
  round(100 * users / user0 , 2 ) as "Retention Rate"
from fine
having round(100 * users / user0 , 2 ) <> 100 
order by 1 desc, 2

    """
    df = pd.read_sql(query, conn)
    return df
# === Load Data: Row 8 ====================================
df_its_user_retention = load_its_user_retention()
# === Chart: Heatmap (Row 8) ==============================
pivot_its_users = df_its_user_retention.pivot_table(index="Cohort Date", columns="Month", values="Retention Rate", aggfunc="sum", fill_value=0)
fig_heatmap_its_users = px.imshow(pivot_its_users, text_auto=True, aspect="auto", color_continuous_scale='Viridis', title="ITS - User Retention")
st.plotly_chart(fig_heatmap_its_users, use_container_width=True)

# --- Row 9 ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_gmp_user_retention():

    query = f"""
    with base as (SELECT  
    data:call.transaction.from::STRING AS tx_signer,
    min(date_trunc('month', created_at)) over (partition by tx_signer) as signup_date,
    date_trunc('month', created_at) as activity_date,
    datediff('month', signup_date, activity_date) as difference
  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed' AND simplified_status = 'received'),
unp as (
  select TO_VARCHAR(signup_date, 'yyyy-MM') as cohort_date, difference as months, count (distinct TX_SIGNER) as users
  from base
  where datediff('month', signup_date, current_date()) <= 24
  group by 1,2
  order by 1),
fine as (select u.*, p.USERS as user0
  from unp u left join unp p on u.COHORT_DATE = p.COHORT_DATE
  where p.MONTHS = 0)
select 
  COHORT_DATE as "Cohort Date", 
  MONTHS as "Month",
  round(100 * users / user0 , 2 ) as "Retention Rate"
from fine
having round(100 * users / user0 , 2 ) <> 100 
order by 1 desc, 2

    """
    df = pd.read_sql(query, conn)
    return df
# === Load Data: Row 9 ====================================
df_gmp_user_retention = load_gmp_user_retention()
# === Chart: Heatmap (Row 9) ==============================
pivot_gmp_users = df_gmp_user_retention.pivot_table(index="Cohort Date", columns="Month", values="Retention Rate", aggfunc="sum", fill_value=0)
fig_heatmap_gmp_users = px.imshow(pivot_gmp_users, text_auto=True, aspect="auto", color_continuous_scale='Viridis', title="GMP - User Retention")
st.plotly_chart(fig_heatmap_gmp_users, use_container_width=True)

# --- Row 10 ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_tt_user_retention():

    query = f"""
    with base as (SELECT  
    SENDER_ADDRESS as TX_SIGNER,
    min(date_trunc('month', created_at)) over (partition by TX_SIGNER) as signup_date,
    date_trunc('month', created_at) as activity_date,
    datediff('month', signup_date, activity_date) as difference
 from axelar.axelscan.fact_transfers
  WHERE status = 'executed' AND simplified_status = 'received'),
unp as (
  select TO_VARCHAR(signup_date, 'yyyy-MM') as cohort_date, difference as months, count (distinct TX_SIGNER) as users
  from base
  where datediff('month', signup_date, current_date()) <= 24
  group by 1,2
  order by 1),
fine as (select u.*, p.USERS as user0
  from unp u left join unp p on u.COHORT_DATE = p.COHORT_DATE
  where p.MONTHS = 0)
select 
  COHORT_DATE as "Cohort Date", 
  MONTHS as "Month",
  round(100 * users / user0 , 2 ) as "Retention Rate"
from fine
having round(100 * users / user0 , 2 ) <> 100 
order by 1 desc, 2

    """
    df = pd.read_sql(query, conn)
    return df
# === Load Data: Row 10 ====================================
df_tt_user_retention = load_tt_user_retention()
# === Chart: Heatmap (Row 10) ==============================
pivot_tt_users = df_tt_user_retention.pivot_table(index="Cohort Date", columns="Month", values="Retention Rate", aggfunc="sum", fill_value=0)
fig_heatmap_tt_users = px.imshow(pivot_tt_users, text_auto=True, aspect="auto", color_continuous_scale='Viridis', title="Token Transfers - User Retention")
st.plotly_chart(fig_heatmap_tt_users, use_container_width=True)
