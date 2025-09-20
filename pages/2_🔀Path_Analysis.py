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
st.title("🔀Path Analysis")

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
        <h2 style="color:#000000; text-align:center;">Analysis of Cross-Chain Paths</h2>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Getting Chains Data from API ---------------------------------------------------------------------------------------

url = "https://api.axelarscan.io/api/getChains"
response = requests.get(url)
chains_data = response.json()
chains_df = pd.DataFrame([
    {
        "Chain ID": chain.get("chain_id"),
        "Name": chain.get("chain_name"),
        "Symbol": chain.get("native_token", {}).get("symbol"),
        "Explorer": chain.get("explorer", {}).get("name"),
        "RPC Endpoints": ", ".join(chain.get("endpoints", {}).get("rpc", [])[:2]) + (" ..." if len(chain.get("endpoints", {}).get("rpc", [])) > 2 else ""),
        "Gateway": chain.get("gateway", {}).get("address"),
        "Type": chain.get("chain_type"),
    }
    for chain in chains_data
])
# --- Row 1: KPIs ----------------------------------------------------------------------------------------------------------------------------------------------------------------
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

SELECT count(distinct (source_chain || '➡' || destination_chain)) as "Unique Paths"
from axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
    """
    df = pd.read_sql(query, conn)
    return df

# === Load Kpi =====================================
df_crosschain_stats = load_crosschain_stats(start_date, end_date)

total_chains = len(chains_df)
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

st.markdown("<br>", unsafe_allow_html=True)
col1, col2 = st.columns(2)
with col1:
    st.markdown(card_style.format(label="🧩Total Supported Chains", value=total_chains), unsafe_allow_html=True)

with col2:
    st.markdown(card_style.format(label="🔀Number of Paths", value=f"{df_crosschain_stats['Unique Paths'][0]:,}"), unsafe_allow_html=True)
    
st.markdown("<br>", unsafe_allow_html=True)
# --- Row 2: Table ---------------------------------------------------------------------------------------------------------------------------------------------------------------------
st.markdown("<h5 style='font-size:18px; margin-bottom:-100px;'>📋 Details of supported chains</h5>", unsafe_allow_html=True)

chains_df.index = chains_df.index + 1
st.markdown("<br>", unsafe_allow_html=True)
st.dataframe(
    chains_df,
    use_container_width=True,
    height=600
)
st.markdown("<br>", unsafe_allow_html=True)
# --- Row 3 ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
st.markdown("<h5 style='font-size:18px; margin-bottom:1px;'>Monitoring Cross-Chain Paths</h5>", unsafe_allow_html=True)

@st.cache_data
def load_path_table(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

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

select (source_chain || '➡' || destination_chain) "🔀Path", 
TO_VARCHAR(count(distinct id), '999,999,999,999,999') as "🚀Number of Transfers", 
TO_VARCHAR(count(distinct user), '999,999,999,999,999') as "👥Number of Users",
'' || '' || TO_VARCHAR(round(sum(amount_usd),2), '999,999,999,999,999') as "💸Volume of Transfers ($USD)",
'' || '' || TO_VARCHAR(round(avg(amount_usd),2), '999,999,999,999,999') as "📊Avg Volume per Txn ($USD)",
'' || '' || TO_VARCHAR(round(sum(fee),2), '999,999,999,999,999') as "⛽Total Fee ($USD)",
'' || '' || round(avg(fee),2) as "🔥Avg Fee ($USD)"
from axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
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
# === Load Data =====================================
df_path_table = load_path_table(start_date, end_date)
# ===================================================
if not df_path_table.empty:
    df_path_table.index = df_path_table.index + 1  # Start index from 1
    st.dataframe(df_path_table, use_container_width=True)
else:
    st.warning("No cross-chain path data available for the selected period.")

# --- Row 4,5 = -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_top_path(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

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

select (source_chain || '➡' || destination_chain) "Path", 
count(distinct id) as "Number of Transfers", 
count(distinct user)as "Number of Users",
round(sum(amount_usd),2) as "Volume of Transfers",
round(sum(fee),2) as "Total Fee"
from axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
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
# === Load Data ===============================================
df_top_path = load_top_path(start_date, end_date)
# === Charts: Row 5,6 =========================================
top_vol = df_top_path.nlargest(10, "Volume of Transfers")
top_txn = df_top_path.nlargest(10, "Number of Transfers")
top_usr = df_top_path.nlargest(10, "Number of Users")
top_fee = df_top_path.nlargest(10, "Total Fee")

def human_format(num):
    if num >= 1_000_000_000:
        return f"{num/1_000_000_000:.1f}B"
    elif num >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}k"
    else:
        return str(num)

def add_bar_labels(fig, x_col, df):
    fig.update_traces(
        text=df[x_col].apply(human_format),
        textposition="inside"   
    )
    return fig

# === Row 4 ===========================================
col1, col2 = st.columns(2)

with col1:
    fig1 = px.bar(top_vol.sort_values("Volume of Transfers"), x="Volume of Transfers", y="Path", orientation="h", title="Top Paths By Volume",
        labels={"Volume of Transfers": "$USD", "Path": ""})
    fig1 = add_bar_labels(fig1, "Volume of Transfers", top_vol)
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = px.bar(top_txn.sort_values("Number of Transfers"), x="Number of Transfers", y="Path", orientation="h", title="Top Paths By Transaction",
        labels={"Number of Transfers": "Txns count", "Path": ""})
    fig2 = add_bar_labels(fig2, "Number of Transfers", top_txn)
    st.plotly_chart(fig2, use_container_width=True)

# === Row 5 ===========================================
col3, col4 = st.columns(2)

with col3:
    fig3 = px.bar(top_usr.sort_values("Number of Users"), x="Number of Users", y="Path", orientation="h", title="Top Paths By User",
        labels={"Number of Users": "wallet count", "Path": ""})
    fig3 = add_bar_labels(fig3, "Number of Users", top_usr)
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    fig4 = px.bar(top_fee.sort_values("Total Fee"), x="Total Fee", y="Path", orientation="h", title="Highest Fee-Collecting Paths",
        labels={"Total Fee": "$USD", "Path": ""})
    fig4 = add_bar_labels(fig4, "Total Fee", top_fee)
    st.plotly_chart(fig4, use_container_width=True)

# --- Row 6 ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
st.markdown("<h5 style='font-size:18px; margin-bottom:1px;'>Monitoring Source Chains</h5>", unsafe_allow_html=True)

@st.cache_data
def load_source_chain_table(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

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

select (source_chain) "📤Source Chain", 
TO_VARCHAR(count(distinct id), '999,999,999,999,999') as "🚀Number of Transfers", 
TO_VARCHAR(count(distinct user), '999,999,999,999,999') as "👥Number of Users",
'' || '' || TO_VARCHAR(round(sum(amount_usd),2), '999,999,999,999,999') as "💸Volume of Transfers ($USD)",
'' || '' || TO_VARCHAR(round(avg(amount_usd),2), '999,999,999,999,999') as "📊Avg Volume per Txn ($USD)",
'' || '' || TO_VARCHAR(round(sum(fee),2), '999,999,999,999,999') as "⛽Total Fee ($USD)",
'' || '' || round(avg(fee),2) as "🔥Avg Fee ($USD)"
from axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
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
# === Load Data =====================================
df_source_chain_table = load_source_chain_table(start_date, end_date)
# ===================================================
if not df_source_chain_table.empty:
    df_source_chain_table.index = df_source_chain_table.index + 1  # Start index from 1
    st.dataframe(df_source_chain_table, use_container_width=True)
else:
    st.warning("No cross-chain path data available for the selected period.")

# --- Row 7,8 = -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_top_source_chains(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

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

select source_chain as "Source Chain", 
count(distinct id) as "Number of Transfers", 
count(distinct user)as "Number of Users",
round(sum(amount_usd),2) as "Volume of Transfers",
round(sum(fee),2) as "Total Fee"
from axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
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
# === Load Data ===============================================
df_top_source_chains = load_top_source_chains(start_date, end_date)
# === Charts: Row 7,8 =========================================
top_vol = df_top_source_chains.nlargest(10, "Volume of Transfers")
top_txn = df_top_source_chains.nlargest(10, "Number of Transfers")
top_usr = df_top_source_chains.nlargest(10, "Number of Users")
top_fee = df_top_source_chains.nlargest(10, "Total Fee")

def human_format(num):
    if num >= 1_000_000_000:
        return f"{num/1_000_000_000:.1f}B"
    elif num >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}k"
    else:
        return str(num)

def add_bar_labels(fig, x_col, df):
    fig.update_traces(
        text=df[x_col].apply(human_format),
        textposition="inside"   
    )
    return fig

# === Row 7 ===========================================
col1, col2 = st.columns(2)

with col1:
    fig1 = px.bar(top_vol.sort_values("Volume of Transfers"), x="Volume of Transfers", y="Source Chain", orientation="h", title="Top Source Chains By Volume",
        labels={"Volume of Transfers": "$USD", "Source Chain": ""})
    fig1 = add_bar_labels(fig1, "Volume of Transfers", top_vol.sort_values("Volume of Transfers"))
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = px.bar(top_txn.sort_values("Number of Transfers"), x="Number of Transfers", y="Source Chain", orientation="h", title="Top Source Chains By Transaction",
        labels={"Number of Transfers": "Txns count", "Source Chain": ""})
    fig2 = add_bar_labels(fig2, "Number of Transfers", top_txn)
    st.plotly_chart(fig2, use_container_width=True)

# === Row 8 ===========================================
col3, col4 = st.columns(2)

with col3:
    fig3 = px.bar(top_usr.sort_values("Number of Users"), x="Number of Users", y="Source Chain", orientation="h", title="Top Source Chains By User",
        labels={"Number of Users": "wallet count", "Source Chain": ""})
    fig3 = add_bar_labels(fig3, "Number of Users", top_usr)
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    fig4 = px.bar(top_fee.sort_values("Total Fee"), x="Total Fee", y="Source Chain", orientation="h", title="Highest Fee-Collecting Source Chains",
        labels={"Total Fee": "$USD", "Source Chain": ""})
    fig4 = add_bar_labels(fig4, "Total Fee", top_fee)
    st.plotly_chart(fig4, use_container_width=True)

