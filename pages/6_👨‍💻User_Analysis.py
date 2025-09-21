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

