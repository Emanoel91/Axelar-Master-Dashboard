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

