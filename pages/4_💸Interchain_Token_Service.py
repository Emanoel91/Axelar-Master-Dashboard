import streamlit as st
import pandas as pd
import requests
import time
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import networkx as nx

st.set_page_config(
    page_title="Axelar Interchain Token Service (ITS)",
    page_icon="https://pbs.twimg.com/profile_images/1869486848646537216/rs71wCQo_400x400.jpg",
    layout="wide"
)

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

tabs = st.tabs(["🚀 Interchain Transfers", "✨ ITS Tokens", "📑 Token Deployments"])

with tabs[0]:
    # === ITS Transfers Analysis ===

    # --- Page Config ------------------------------------------------------------------------------------------------------


    # --- Title -----------------------------------------------------------------------------------------------------
    st.title("🚀Interchain Transfers")

    st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
    st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

    # --- Sidebar Footer Slightly Left-Aligned ---


    # --- Snowflake Connection ----------------------------------------------------------------------------------------
    snowflake_secrets = st.secrets['snowflake']
    user = snowflake_secrets['user']
    account = snowflake_secrets['account']
    private_key_str = snowflake_secrets['private_key']
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
        timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"], key="selectbox_1")

    with col2:
        start_date = st.date_input("Start Date", value=pd.to_datetime("2023-12-01", key="date_input_1"))

    with col3:
        end_date = st.date_input("End Date", value=pd.to_datetime("2025-09-30", key="date_input_4"))
    # --- Fetch Data from APIs --------------------------------------------------------------------------------------------------------
    @st.cache_data
    def load_interchain_stats(start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        WITH axelar_service AS (

      SELECT  created_at, LOWER(data:call.chain::STRING) AS source_chain, LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
        data:call.transaction.from::STRING AS user, CASE 
          WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
          WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
          ELSE NULL
        END AS amount, CASE 
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
            ELSE NULL END) AS fee, id, data:symbol::STRING AS Symbol
      FROM axelar.axelscan.fact_gmp 
      WHERE status = 'executed' AND simplified_status = 'received' AND (
            data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
            or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
            ))

    SELECT count(distinct user) as "Unique Users", count(distinct (source_chain || '➡' || destination_chain)) as "Paths", 
    count(distinct symbol) as "Tokens", round(sum(fee)) as "Total Transfer Fees"
    FROM axelar_service
    where created_at::date>='{start_str}' and created_at::date<='{end_str}'
        """

        df = pd.read_sql(query, conn)
        return df

    # --- Load Data --------------------------------------------------------------------------------------------------------------------
    df_interchain_stats = load_interchain_stats(start_date, end_date)
    # ---Axelarscan api ----------------------------------------------------------------------------------------------------------------
    api_urls = [
        "https://api.axelarscan.io/gmp/GMPChart?contractAddress=0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C",
        "https://api.axelarscan.io/gmp/GMPChart?contractAddress=axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr"
    ]

    dfs = []
    for url in api_urls:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['data']
            df = pd.DataFrame(data)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            dfs.append(df)
        else:
            st.error(f"Failed to fetch data from {url}")

    # --- Combine and Filter ------------------------------------------------------------------------------------------------
    df_all = pd.concat(dfs)
    df_all = df_all[(df_all['timestamp'].dt.date >= start_date) & (df_all['timestamp'].dt.date <= end_date)]

    # --- Aggregate by Timeframe ----------------------------------------------------------------------------------------
    if timeframe == "week":
        df_all['period'] = df_all['timestamp'].dt.to_period("W").apply(lambda r: r.start_time)
    elif timeframe == "month":
        df_all['period'] = df_all['timestamp'].dt.to_period("M").apply(lambda r: r.start_time)
    else:
        df_all['period'] = df_all['timestamp']

    agg_df = df_all.groupby("period").agg({
        "num_txs": "sum",
        "volume": "sum"
    }).reset_index()

    agg_df = agg_df.sort_values("period")
    agg_df['cum_num_txs'] = agg_df['num_txs'].cumsum()
    agg_df['cum_volume'] = agg_df['volume'].cumsum()

    # --- KPIs -----------------------------------------------------------------------------------------------------------
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
        st.markdown(card_style.format(label="Total Number of Transfers", value=f"{agg_df['num_txs'].sum():,} Txns"), unsafe_allow_html=True)

    with col2:
        st.markdown(card_style.format(label="Total Volume of Transfers", value=f"${round(agg_df['volume'].sum()):,}"), unsafe_allow_html=True)

    with col3:
        st.markdown(card_style.format(label="Unique Users", value=f"{df_interchain_stats['Unique Users'][0]:,} Wallets"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    col4, col5, col6 = st.columns(3)
    with col4:
        st.markdown(card_style.format(label="Unique Paths", value=f"{df_interchain_stats['Paths'][0]:,}"), unsafe_allow_html=True)

    with col5:
        st.markdown(card_style.format(label="#Tokens (with Volume>0$)", value=f"{df_interchain_stats['Tokens'][0]:,}"), unsafe_allow_html=True)

    with col6:
        st.markdown(card_style.format(label="Total Transfer Fees", value=f"${df_interchain_stats['Total Transfer Fees'][0]:,}"), unsafe_allow_html=True)

    # --- Plots ----------------------------------------------------------------------------------------------------------
    col1, col2 = st.columns(2)

    # Number of Interchain Transfers Over Time
    fig1 = go.Figure()
    fig1.add_trace(go.Bar(x=agg_df['period'], y=agg_df['num_txs'], name="Transfers", yaxis="y1", marker_color="#ff7f27"))
    fig1.add_trace(go.Scatter(x=agg_df['period'], y=agg_df['cum_num_txs'], name="Total Transfers", yaxis="y2", mode="lines", line=dict(color="black")))
    fig1.update_layout(title="Number of Interchain Transfers Over Time", yaxis=dict(title="Txns count"), yaxis2=dict(title="Txns count", overlaying="y", side="right"),
        xaxis_title="", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
    col1.plotly_chart(fig1, use_container_width=True)

    # Volume of Interchain Transfers Over Time
    fig2 = go.Figure()
    fig2.add_trace(go.Bar(x=agg_df['period'], y=agg_df['volume'], name="Volume", yaxis="y1", marker_color="#ff7f27"))
    fig2.add_trace(go.Scatter(x=agg_df['period'], y=agg_df['cum_volume'],name="Total Volume", yaxis="y2", mode="lines", line=dict(color="black")))
    fig2.update_layout(title="Volume of Interchain Transfers Over Time", yaxis=dict(title="$USD"), yaxis2=dict(title="$USD", overlaying="y", side="right"), xaxis_title="",
        legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
    col2.plotly_chart(fig2, use_container_width=True)


    @st.cache_data
    def load_interchain_users_data(timeframe, start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        with table1 as (
        WITH tab1 AS (
        SELECT data:call.transaction.from::STRING AS user, min(created_at::date) as first_txn_date
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed' AND simplified_status = 'received' AND (
            data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
            or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
            )
        group by 1)
        select date_trunc('{timeframe}',first_txn_date) as "Date", count(distinct user) as "New Users", sum("New Users") over (order by "Date") as "User Growth"
        from tab1 
        where first_txn_date>='{start_str}' and first_txn_date<='{end_str}'
        group by 1),
        table2 as (SELECT date_trunc('{timeframe}',created_at) as "Date", count(distinct data:call.transaction.from::STRING) AS "Total Users"
        FROM axelar.axelscan.fact_gmp 
        WHERE created_at::date>='{start_str}' and created_at::date<='{end_str}' and status = 'executed' AND simplified_status = 'received' AND (
        data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
        or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
            ) 
        group by 1)
        select table1."Date" as "Date", "New Users", "Total Users", "Total Users"-"New Users" as "Returning Users", "User Growth",
        round((("New Users"/"Total Users")*100),1) as "%Growth Rate"
        from table1 left join table2 on table1."Date"=table2."Date"
        order by 1
        """

        df = pd.read_sql(query, conn)
        return df

    @st.cache_data
    def load_interchain_fees_data(timeframe, start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        WITH axelar_service AS (  
        SELECT  
        created_at, COALESCE(CASE 
            WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
              OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
            THEN NULL
            WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
              AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
            THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
            ELSE NULL
          END, CASE 
            WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
            WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
            ELSE NULL
          END) AS fee,
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed'
        AND simplified_status = 'received'
        AND (
            data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
            or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
            ))
        SELECT date_trunc('{timeframe}',created_at) as "Date", round(sum(fee)) as "Transfer Fees", sum("Transfer Fees") over (order by "Date") as "Total Transfer Fees",
        round(avg(fee),3) as "Average Gas Fee", round(median(fee),3) as "Median Gas Fee"
        FROM axelar_service
        where created_at::date>='{start_str}' and created_at::date<='{end_str}'
        group by 1
        order by 1
        """

        df = pd.read_sql(query, conn)
        return df

    @st.cache_data
    def load_interchain_fees_stats(start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        WITH axelar_service AS (

      SELECT  
        created_at, COALESCE(CASE 
            WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
              OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
            THEN NULL
            WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
              AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
            THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
            ELSE NULL
          END, CASE 
            WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
            WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
            ELSE NULL
          END) AS fee,
      FROM axelar.axelscan.fact_gmp 
      WHERE status = 'executed'
        AND simplified_status = 'received'
        AND (
            data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
            or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
            ) 
    )

    SELECT round(avg(fee),2) as "Average Gas Fee", round(median(fee),2) as "Median Gas Fee"
    FROM axelar_service
    where created_at::date>='{start_str}' and created_at::date<='{end_str}'
        """

        df = pd.read_sql(query, conn)
        return df

    # --- Load Data --------------------------------------------------------------------------------------------------------------------
    df_interchain_users_data = load_interchain_users_data(timeframe, start_date, end_date)
    df_interchain_fees_data = load_interchain_fees_data(timeframe, start_date, end_date)
    df_interchain_fees_stats = load_interchain_fees_stats(start_date, end_date)
    # ----------------------------------------------------------------------------------------------------------------------------------
    col1, col2 = st.columns(2)

    with col1:
        fig_b1 = go.Figure()
        # Stacked Bars
        fig_b1.add_trace(go.Bar(x=df_interchain_users_data['Date'], y=df_interchain_users_data['New Users'], name="New Users", marker_color="#0ed145"))
        fig_b1.add_trace(go.Bar(x=df_interchain_users_data['Date'], y=df_interchain_users_data['Returning Users'], name="Returning Users", marker_color="#ff7f27"))
        fig_b1.add_trace(go.Scatter(x=df_interchain_users_data['Date'], y=df_interchain_users_data['Total Users'], name="Total Users", mode="lines", line=dict(color="black", width=2)))
        fig_b1.update_layout(barmode="stack", title="Number of Users Over Time", yaxis=dict(title="Wallet count"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5))
        st.plotly_chart(fig_b1, use_container_width=True)

    with col2:
        fig2 = px.area(df_interchain_users_data, x="Date", y="User Growth", title="Interchain Users Growth Over Time", color_discrete_sequence=['#ff7f27'])
        fig2.add_trace(go.Scatter(x=df_interchain_users_data['Date'], y=df_interchain_users_data['%Growth Rate'], name="%Growth Rate", mode="lines", yaxis="y2", line=dict(color="black")))
        fig2.update_layout(xaxis_title="", yaxis_title="wallet count",  yaxis2=dict(title="%", overlaying="y", side="right"), template="plotly_white")
        st.plotly_chart(fig2, use_container_width=True)

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

    col3, col4 = st.columns(2)
    with col3:
        st.markdown(card_style.format(label="Average Gas Fee", value=f"${df_interchain_fees_stats['Average Gas Fee'][0]:,}"), unsafe_allow_html=True)

    with col4:
        st.markdown(card_style.format(label="Median Gas Fee", value=f"${df_interchain_fees_stats['Median Gas Fee'][0]:,}"), unsafe_allow_html=True)


    col5, col6 = st.columns(2)

    with col5:
        fig5 = go.Figure()
        fig5.add_bar(x=df_interchain_fees_data['Date'], y=df_interchain_fees_data['Transfer Fees'], name="Fee", yaxis="y1", marker_color="#ff7f27")
        fig5.add_trace(go.Scatter(x=df_interchain_fees_data['Date'], y=df_interchain_fees_data['Total Transfer Fees'], name="Total Fees", mode="lines", 
                                  yaxis="y2", line=dict(color="black")))
        fig5.update_layout(title="Interchain Transfer Fees Over Time", yaxis=dict(title="$USD"), yaxis2=dict(title="$USD", overlaying="y", side="right"), xaxis=dict(title=""),
            barmode="group", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
        st.plotly_chart(fig5, use_container_width=True)

    with col6:
        fig6 = go.Figure()
        fig6.add_trace(go.Scatter(x=df_interchain_fees_data['Date'], y=df_interchain_fees_data['Average Gas Fee'], name="Avg Gas Fee", mode="lines", 
                                  yaxis="y1", line=dict(color="blue")))
        fig6.add_trace(go.Scatter(x=df_interchain_fees_data['Date'], y=df_interchain_fees_data['Median Gas Fee'], name="Median Gas Fee", mode="lines", 
                                  yaxis="y2", line=dict(color="green")))
        fig6.update_layout(title="Average & Median Transfer Fees Over Time", yaxis=dict(title="$USD"), yaxis2=dict(title="$USD", overlaying="y", side="right"), xaxis=dict(title=""),
            barmode="group", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
        st.plotly_chart(fig6, use_container_width=True)

    # --------------------------------------------------------------------------------------------------------------------------------------------------------
    # --- Chains Analysis-------------------------------------------------------------------------------------------------------------------------------------
    # --------------------------------------------------------------------------------------------------------------------------------------------------------

    def to_timestamp(date):
        return int(pd.Timestamp(date).timestamp())

    @st.cache_data
    def load_chain_stats(start_date, end_date):
        from_time = to_timestamp(start_date)
        to_time = to_timestamp(end_date)

        api_urls = [
            f"https://api.axelarscan.io/gmp/GMPStatsByChains?contractAddress=0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C&fromTime={from_time}&toTime={to_time}",
            f"https://api.axelarscan.io/gmp/GMPStatsByChains?contractAddress=axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr&fromTime={from_time}&toTime={to_time}"
        ]

        all_sources = []
        all_destinations = []
        all_paths = []

        for url in api_urls:
            resp = requests.get(url)
            if resp.status_code == 200:
                data = resp.json()['source_chains']
                for s in data:
                    # source chain aggregation
                    all_sources.append({
                        "source_chain": s['key'],
                        "num_txs": s.get("num_txs", 0),
                        "volume": s.get("volume", 0.0)
                    })
                    # destination chain aggregation
                    for d in s['destination_chains']:
                        all_destinations.append({
                            "destination_chain": d['key'],
                            "num_txs": d.get("num_txs", 0),
                            "volume": d.get("volume", 0.0)
                        })
                        # paths aggregation
                        all_paths.append({
                            "path": f"{s['key']} ➡ {d['key']}",
                            "num_txs": d.get("num_txs", 0),
                            "volume": d.get("volume", 0.0)
                        })

        df_sources = pd.DataFrame(all_sources).groupby("source_chain", as_index=False).sum()
        df_destinations = pd.DataFrame(all_destinations).groupby("destination_chain", as_index=False).sum()
        df_paths = pd.DataFrame(all_paths).groupby("path", as_index=False).sum()

        return df_sources, df_destinations, df_paths

    # ------- Source Chains: Snowflake ------------------------------------
    @st.cache_data
    def load_source_chains_stats(start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        WITH axelar_service AS (

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
        data:symbol::STRING AS Symbol

      FROM axelar.axelscan.fact_gmp 
      WHERE status = 'executed'
        AND simplified_status = 'received'
        AND (
            data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
            or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
            ) 
    )

    SELECT source_chain as "Source Chain", count(distinct user) as "Number of Users"
    FROM axelar_service
    where created_at::date>='{start_str}' and created_at::date<='{end_str}'
    group by 1
    order by 2 desc 
        """

        df = pd.read_sql(query, conn)
        return df

    # ------- Top 5: Source Chains: Snowflake ------------------------------------
    @st.cache_data
    def load_Top_source_chains_stats(start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        WITH axelar_service AS (

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
        data:symbol::STRING AS Symbol

      FROM axelar.axelscan.fact_gmp 
      WHERE status = 'executed'
        AND simplified_status = 'received'
        AND (
            data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
            or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
            ) 
    )

    SELECT source_chain as "Source Chain", count(distinct user) as "Number of Users"
    FROM axelar_service
    where created_at::date>='{start_str}' and created_at::date<='{end_str}'
    group by 1
    order by 2 desc 
    limit 5
        """

        df = pd.read_sql(query, conn)
        return df

    # --- Load Data ---------------------------------------------------------------
    df_sources, df_destinations, df_paths = load_chain_stats(start_date, end_date)
    df_source_chains_stats = load_source_chains_stats(start_date, end_date)
    df_Top_source_chains_stats = load_Top_source_chains_stats(start_date, end_date)

    # === Source Chains Tables ===================================================
    col1, col2, col3 = st.columns(3)

    # Source Chains by Transactions
    with col1:
        st.markdown("<h5 style='font-size:18px; font-weight:bold;'>🔗 Source Chains by Transactions</h5>", unsafe_allow_html=True)
        df_display1 = df_sources[["source_chain", "num_txs"]].copy()
        df_display1 = df_display1.sort_values("num_txs", ascending=False).reset_index(drop=True)
        df_display1.index = df_display1.index + 1  
        df_display1['num_txs'] = df_display1['num_txs'].apply(lambda x: f"{x:,}")  
        df_display1 = df_display1.rename(columns={
            "source_chain": "Source Chain",
            "num_txs": "Number of Transfers"
        })
        st.dataframe(df_display1, use_container_width=True)

    # Source Chains by Volume
    with col2:
        st.markdown("<h5 style='font-size:18px; font-weight:bold;'>💸 Source Chains by Volume</h5>", unsafe_allow_html=True)
        df_display2 = df_sources[["source_chain", "volume"]].copy()
        df_display2 = df_display2.sort_values("volume", ascending=False).reset_index(drop=True)
        df_display2.index = df_display2.index + 1  
        df_display2['volume'] = df_display2['volume'].apply(lambda x: f"{x:,.2f}") 
        df_display2 = df_display2.rename(columns={
            "source_chains": "Source Chains",
            "volume": "Volume of Transfers ($USD)"
        })
        st.dataframe(df_display2, use_container_width=True)

    # Source Chains by Users
    with col3:
        st.markdown("<h5 style='font-size:18px; font-weight:bold;'>👥 Source Chains by Users</h5>", unsafe_allow_html=True)
        df_display3 = df_source_chains_stats.copy()
        df_display3.index = df_display3.index + 1
        df_display3 = df_display3.applymap(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)
        st.dataframe(df_display3, use_container_width=True)

    # === Source Chains Charts ===================================================================================
    col1, col2, col3 = st.columns(3)
    with col1:
        top5 = df_sources.sort_values("num_txs", ascending=False).head(5)
        fig = px.bar(top5, x="source_chain", y="num_txs", title="Top 5 Source Chains by Transactions", text="num_txs", labels={"source_chain": "", "num_txs": "Txns count"})
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        top5 = df_sources.sort_values("volume", ascending=False).head(5).copy()
        top5['volume'] = top5['volume'].round(0)
        fig = px.bar(top5, x="source_chain", y="volume", title="Top 5 Source Chains by Volume", text="volume", labels={"source_chain": "", "volume": "$USD"})
        st.plotly_chart(fig, use_container_width=True)
    with col3:
        fig = px.bar(df_Top_source_chains_stats, x="Source Chain", y="Number of Users", title="Top 5 Source Chains by Users", text="Number of Users", 
                     labels={"Source Chain": "", "Number of Users": "Wallet count"})
        st.plotly_chart(fig, use_container_width=True)

    # ------- Destination Chains: Snowflake ------------------------------------
    @st.cache_data
    def load_destination_chains_stats(start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        WITH axelar_service AS (

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
        data:symbol::STRING AS Symbol

      FROM axelar.axelscan.fact_gmp 
      WHERE status = 'executed'
        AND simplified_status = 'received'
        AND (
            data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
            or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
            ) 
    )

    SELECT destination_chain as "Destination Chain", count(distinct user) as "Number of Users"
    FROM axelar_service
    where created_at::date>='{start_str}' and created_at::date<='{end_str}'
    group by 1
    order by 2 desc 
        """

        df = pd.read_sql(query, conn)
        return df

    # ------- Top 5: Destination Chains: Snowflake ------------------------------------
    @st.cache_data
    def load_top_destination_chains_stats(start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        WITH axelar_service AS (

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
        data:symbol::STRING AS Symbol

      FROM axelar.axelscan.fact_gmp 
      WHERE status = 'executed'
        AND simplified_status = 'received'
        AND (
            data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
            or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
            ) 
    )

    SELECT destination_chain as "Destination Chain", count(distinct user) as "Number of Users"
    FROM axelar_service
    where created_at::date>='{start_str}' and created_at::date<='{end_str}'
    group by 1
    order by 2 desc 
    limit 5
        """

        df = pd.read_sql(query, conn)
        return df
    # --- Load Data -------------------------------------------------------------------------
    df_destination_chains_stats = load_destination_chains_stats(start_date, end_date)
    df_top_destination_chains_stats = load_top_destination_chains_stats(start_date, end_date)

    # === Destination Chains Tables =========================================================
    col1, col2, col3 = st.columns(3)

    # Destination Chains by Transactions
    with col1:
        st.markdown("<h5 style='font-size:16px; font-weight:bold;'>🔗 Destination Chains by Transactions</h5>", unsafe_allow_html=True)
        df_display1 = df_destinations[["destination_chain", "num_txs"]].copy()
        df_display1 = df_display1.sort_values("num_txs", ascending=False).reset_index(drop=True)
        df_display1.index = df_display1.index + 1  
        df_display1['num_txs'] = df_display1['num_txs'].apply(lambda x: f"{x:,}")  
        df_display1 = df_display1.rename(columns={
            "destination_chain": "Destination Chain",
            "num_txs": "Number of Transfers"
        })
        st.dataframe(df_display1, use_container_width=True)

    # Destination Chains by Volume
    with col2:
        st.markdown("<h5 style='font-size:18px; font-weight:bold;'>💸 Destination Chains by Volume</h5>", unsafe_allow_html=True)
        df_display2 = df_destinations[["destination_chain", "volume"]].copy()
        df_display2 = df_display2.sort_values("volume", ascending=False).reset_index(drop=True)
        df_display2.index = df_display2.index + 1  
        df_display2['volume'] = df_display2['volume'].apply(lambda x: f"{x:,.2f}") 
        df_display2 = df_display2.rename(columns={
            "destination_chain": "Destination Chain",
            "volume": "Volume of Transfers ($USD)"
        })
        st.dataframe(df_display2, use_container_width=True)

    # Destination Chains by Users
    with col3:
        st.markdown("<h5 style='font-size:18px; font-weight:bold;'>👥 Destination Chains by Users</h5>", unsafe_allow_html=True)
        df_display3 = df_destination_chains_stats.copy()
        df_display3.index = df_display3.index + 1
        df_display3 = df_display3.applymap(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)
        st.dataframe(df_display3, use_container_width=True)

    # === Destination Chains Charts ==============================================================================================
    col1, col2, col3 = st.columns(3)
    with col1:
        top5 = df_destinations.sort_values("num_txs", ascending=False).head(5)
        fig = px.bar(top5, x="destination_chain", y="num_txs", title="Top 5 Destination Chains by Transactions", text="num_txs", labels={"destination_chain": "", "num_txs": "Txns count"})
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        top5 = df_destinations.sort_values("volume", ascending=False).head(5).copy()
        top5['volume'] = top5['volume'].round(0)
        fig = px.bar(top5, x="destination_chain", y="volume", title="Top 5 Destination Chains by Volume", text="volume", labels={"destination_chain": "", "volume": "$USD"})
        st.plotly_chart(fig, use_container_width=True)
    with col3:
        fig = px.bar(df_top_destination_chains_stats, x="Destination Chain", y="Number of Users", title="Top 5 Destination Chains by Users", text="Number of Users", 
                     labels={"Destination Chain": "", "Number of Users": "Wallet count"})
        st.plotly_chart(fig, use_container_width=True)

    # ------- Path: Snowflake --------------------------------------------------------------------------------------------------------
    @st.cache_data
    def load_paths_stats(start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        WITH axelar_service AS (

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
        data:symbol::STRING AS Symbol

      FROM axelar.axelscan.fact_gmp 
      WHERE status = 'executed'
        AND simplified_status = 'received'
        AND (
            data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
            or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
            ) 
    )

    SELECT (source_chain || '➡' || destination_chain) as "Path", count(distinct user) as "Number of Users"
    FROM axelar_service
    where created_at::date>='{start_str}' and created_at::date<='{end_str}'
    group by 1
    order by 2 desc 
        """

        df = pd.read_sql(query, conn)
        return df

    # ------- Top 5: Paths: Snowflake ------------------------------------
    @st.cache_data
    def load_top_paths_stats(start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        WITH axelar_service AS (

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
        data:symbol::STRING AS Symbol

      FROM axelar.axelscan.fact_gmp 
      WHERE status = 'executed'
        AND simplified_status = 'received'
        AND (
            data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
            or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
            ) 
    )

    SELECT (source_chain || '➡' || destination_chain) as "Path", count(distinct user) as "Number of Users"
    FROM axelar_service
    where created_at::date>='{start_str}' and created_at::date<='{end_str}'
    group by 1
    order by 2 desc 
    limit 5
        """

        df = pd.read_sql(query, conn)
        return df
    # --- Load Data -------------------------------------------------------------------------
    df_paths_stats = load_paths_stats(start_date, end_date)
    df_top_paths_stats = load_top_paths_stats(start_date, end_date)

    # === Paths Tables ======================================================================
    col1, col2, col3 = st.columns(3)

    # Paths by Transactions
    with col1:
        st.markdown("<h5 style='font-size:18px; font-weight:bold;'>🔗 Paths by Transactions</h5>", unsafe_allow_html=True)
        df_display1 = df_paths[["path", "num_txs"]].copy()
        df_display1 = df_display1.sort_values("num_txs", ascending=False).reset_index(drop=True)
        df_display1.index = df_display1.index + 1  
        df_display1['num_txs'] = df_display1['num_txs'].apply(lambda x: f"{x:,}")  
        df_display1 = df_display1.rename(columns={
            "path": "Path",
            "num_txs": "Number of Transfers"
        })
        st.dataframe(df_display1, use_container_width=True)

    # Paths by Volume
    with col2:
        st.markdown("<h5 style='font-size:18px; font-weight:bold;'>💸 Paths by Volume</h5>", unsafe_allow_html=True)
        df_display2 = df_paths[["path", "volume"]].copy()
        df_display2 = df_display2.sort_values("volume", ascending=False).reset_index(drop=True)
        df_display2.index = df_display2.index + 1  
        df_display2['volume'] = df_display2['volume'].apply(lambda x: f"{x:,.2f}") 
        df_display2 = df_display2.rename(columns={
            "path": "Path",
            "volume": "Volume of Transfers ($USD)"
        })
        st.dataframe(df_display2, use_container_width=True)

    # Paths by Users
    with col3:
        st.markdown("<h5 style='font-size:18px; font-weight:bold;'>👥 Paths by Users</h5>", unsafe_allow_html=True)
        df_display3 = df_paths_stats.copy()
        df_display3.index = df_display3.index + 1
        df_display3 = df_display3.applymap(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)
        st.dataframe(df_display3, use_container_width=True)

    # === Paths Charts ===================================================================================
    col1, col2, col3 = st.columns(3)
    with col1:
        top5 = df_paths.sort_values("num_txs", ascending=False).head(5)
        fig = px.bar(top5, x="path", y="num_txs", title="Top 5 Paths by Transactions", text="num_txs", labels={"path": "", "num_txs": "Txns count"})
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        top5 = df_paths.sort_values("volume", ascending=False).head(5).copy()
        top5['volume'] = top5['volume'].round(0)
        fig = px.bar(top5, x="path", y="volume", title="Top 5 Paths by Volume", text="volume", labels={"path": "", "volume": "$USD"})
        st.plotly_chart(fig, use_container_width=True)
    with col3:
        fig = px.bar(df_top_paths_stats, x="Path", y="Number of Users", title="Top 5 Paths by Users", text="Number of Users", labels={"Path": "", "Number of Users": "Wallet count"})
        st.plotly_chart(fig, use_container_width=True)

with tabs[1]:
    # === ITS Tokens ===

    # --- Page Config ------------------------------------------------------------------------------------------------------


    # --- Title -----------------------------------------------------------------------------------------------------
    st.title("✨ITS Tokens")

    st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")

    # --- Sidebar Footer Slightly Left-Aligned ---


    # --- Convert date to unix (sec) ----------------------------------------------------------------------------------
    def to_unix_timestamp(dt):
        return int(time.mktime(dt.timetuple()))

    # --- Getting APIs -----------------------------------------------------------------------------------------
    @st.cache_data
    def load_data(start_date, end_date):
        from_time = to_unix_timestamp(pd.to_datetime(start_date))
        to_time = to_unix_timestamp(pd.to_datetime(end_date))

        url_tx = f"https://api.axelarscan.io/gmp/GMPTopITSAssets?fromTime={from_time}&toTime={to_time}"
        tx_data = requests.get(url_tx).json().get("data", [])

        url_assets = "https://api.axelarscan.io/api/getITSAssets"
        assets_data = requests.get(url_assets).json()

        address_to_symbol = {}
        symbol_to_image = {}
        for asset in assets_data:
            symbol = asset.get("symbol", "")
            image = asset.get("image", "")
            symbol_to_image[symbol] = image
            addresses = asset.get("addresses", [])
            if isinstance(addresses, str):
                try:
                    addresses = eval(addresses)
                except:
                    addresses = []
            for addr in addresses:
                address_to_symbol[addr.lower()] = symbol

        df = pd.DataFrame(tx_data)
        if df.empty:
            return pd.DataFrame(columns=["Token Address", "Symbol", "Logo", "Number of Transfers", "Volume of Transfers"]), {}

        df['Token Address'] = df['key']
        df['Symbol'] = df['key'].str.lower().map(address_to_symbol).fillna("Unknown")
        df['Logo'] = df['Symbol'].map(symbol_to_image).fillna("")
        df['Number of Transfers'] = df['num_txs'].astype(int)
        df['Volume of Transfers'] = df['volume'].astype(float)

        df = df[["Token Address", "Symbol", "Logo", "Number of Transfers", "Volume of Transfers"]]

        return df, symbol_to_image

    # --- Main Run ------------------------------------------------------------------------------------------------------


    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=pd.to_datetime("2023-12-01", key="date_input_2"))
    with col2:
        end_date = st.date_input("End Date", value=pd.to_datetime("2025-09-30", key="date_input_5"))

    df, symbol_to_image = load_data(start_date, end_date)

    if df.empty:
        st.warning("⛔ No data available for the selected time range.")
    else:

        df_display = df.copy()
        df_display['Number of Transfers'] = df_display['Number of Transfers'].map("{:,}".format)
        df_display['Volume of Transfers'] = df_display['Volume of Transfers'].map("{:,.0f}".format)

        def logo_html(url):
            if url:
                return f'<img src="{url}" style="width:20px;height:20px;border-radius:50%;">'
            return ""

        df_display['Logo'] = df_display['Logo'].apply(logo_html)

        st.subheader("📑 Interchain Token Transfers Table")

        scrollable_table = f"""
        <div style="max-height:700px; overflow-y:auto;">
            {df_display.to_html(escape=False, index=False)}
        </div>
        """

        st.write(scrollable_table, unsafe_allow_html=True)

        # --- chart 1: Top 10 by Volume (without Unknown) -------------------------------------------------------------------
        df_grouped = (
            df[df['Symbol'] != "Unknown"]
            .groupby("Symbol", as_index=False)
            .agg({
                "Number of Transfers": "sum",
                "Volume of Transfers": "sum"
            })
        )

        top_volume = df_grouped.sort_values("Volume of Transfers", ascending=False).head(10)
        fig1 = px.bar(
            top_volume,
            x="Symbol",
            y="Volume of Transfers",
            text="Volume of Transfers",
            color="Symbol"
        )
        fig1.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
        fig1.update_layout(
            title="Top 10 Tokens by Interchain Transfers Volume",
            xaxis_title=" ",
            yaxis_title="$USD",
            showlegend=False
        )

        # --- chart2: Top 10 by Transfers Count (without Unknown + volume > 0) ------------------------------------------------
        df_nonzero = df_grouped[df_grouped['Volume of Transfers'] > 0]
        top_transfers = df_nonzero.sort_values("Number of Transfers", ascending=False).head(10)

        fig2 = px.bar(
            top_transfers,
            x="Symbol",
            y="Number of Transfers",
            text="Number of Transfers",
            color="Symbol"
        )
        fig2.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
        fig2.update_layout(
            title="Top 10 Tokens by Interchain Transfers Count",
            xaxis_title=" ",
            yaxis_title="Transfers count",
            showlegend=False
        )

        st.plotly_chart(fig1, use_container_width=True)
        st.plotly_chart(fig2, use_container_width=True)

with tabs[2]:
    # === ITS Token Deployment ===

    # --- Page Config ------------------------------------------------------------------------------------------------------


    # --- Title -----------------------------------------------------------------------------------------------------
    st.title("📑Token Deployments")

    st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
    st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

    # --- Sidebar Footer Slightly Left-Aligned ---


    # --- Snowflake Connection ----------------------------------------------------------------------------------------
    snowflake_secrets = st.secrets['snowflake']
    user = snowflake_secrets['user']
    account = snowflake_secrets['account']
    private_key_str = snowflake_secrets['private_key']
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
        timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"], key="selectbox_2")

    with col2:
        start_date = st.date_input("Start Date", value=pd.to_datetime("2023-12-01", key="date_input_3"))

    with col3:
        end_date = st.date_input("End Date", value=pd.to_datetime("2025-09-30", key="date_input_6"))


    # --- Row 1 ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    @st.cache_data
    def load_deploy_stats(start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        with table1 as (
    SELECT data:interchain_token_deployment_started:tokenId as token, 
    data:call:transaction:from as deployer, COALESCE(CASE 
            WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
              OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
            THEN NULL
            WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
              AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
            THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
            ELSE NULL
          END, CASE 
            WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
            WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
            ELSE NULL
          END) AS fee
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed' AND simplified_status = 'received' AND (
    data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
    or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
    ) AND data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
    and created_at::date>='{start_str}' and created_at::date<='{end_str}')

    select count(distinct token) as "Total Number of Deployed Tokens",
    count(distinct deployer) as "Total Number of Token Deployers",
    round(sum(fee)) as "Total Gas Fees"
    from table1

        """

        df = pd.read_sql(query, conn)
        return df

    # === Load Data: Row 1 =================================================
    df_deploy_stats = load_deploy_stats(start_date, end_date)
    # === KPIs: Row 1 ======================================================
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
        st.markdown(card_style.format(label="Number of Deployed Tokens", value=f"✨{df_deploy_stats['Total Number of Deployed Tokens'][0]:,}"), unsafe_allow_html=True)
    with col2:
        st.markdown(card_style.format(label="Number of Token Deployers", value=f"👨‍💻{df_deploy_stats['Total Number of Token Deployers'][0]:,}"), unsafe_allow_html=True)
    with col3:
        st.markdown(card_style.format(label="Total Gas Fees", value=f"⛽${df_deploy_stats['Total Gas Fees'][0]:,}"), unsafe_allow_html=True)

    # --- Row 2: Number of Deployer --------------------------------------------------------------------------------------------------------------------------------------------
    @st.cache_data
    def load_deployers_overtime(timeframe, start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        with table1 as (SELECT date_trunc('{timeframe}',created_at) as "Date", count(distinct data:call:transaction:from) as "Total Deployers"
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed' AND simplified_status = 'received' AND (
    data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
    or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
    ) AND data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
    and created_at::date>='{start_str}' and created_at::date<='{end_str}'
    group by 1
    order by 1),

    table2 as (with tab1 as (
    SELECT data:call:transaction:from as deployer, min(created_at::date) as first_deployment_date
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed' AND simplified_status = 'received' AND (
    data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
    or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
    ) AND data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
    group by 1)

    select date_trunc('{timeframe}',first_deployment_date) as "Date", count(distinct deployer) as "New Deployers"
    from tab1
    where first_deployment_date>='{start_str}' and first_deployment_date<='{end_str}'
    group by 1)

    select table1."Date" as "Date", "Total Deployers", "New Deployers", "Total Deployers"-"New Deployers" as "Returning Deployers"
    from table1 left join table2 on table1."Date"=table2."Date"
    order by 1

        """

        df = pd.read_sql(query, conn)
        return df

    # --- Row 2: Number of Tokens Deployed ----------------------------------------------------------------------------------------------------------------------------------------------
    @st.cache_data
    def load_deployed_tokens(timeframe, start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        SELECT date_trunc('{timeframe}',created_at) as "Date", count(distinct data:interchain_token_deployment_started:tokenId) as "Number of Tokens", case 
    when (call:receipt:logs[0]:address ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' or 
    call:receipt:logs[0]:address ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%') then 'Existing Tokens'
    else 'Newly Minted Token' end as "Token Type"
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed' AND simplified_status = 'received' AND (
    data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
    or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
    ) AND data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
    AND created_at::date>='{start_str}' and created_at::date<='{end_str}'
    group by 1, 3 
    order by 1

        """

        df = pd.read_sql(query, conn)
        return df

    # === Load Data: Row 2 ====================================================================
    df_deployers_overtime = load_deployers_overtime(timeframe, start_date, end_date)
    df_deployed_tokens = load_deployed_tokens(timeframe, start_date, end_date)
    # === Chart: Row 2 ========================================================================
    color_map = {
        "Existing Tokens": "#858dff",
        "Newly Minted Token": "#fc9047"
    }

    col1, col2 = st.columns(2)

    with col1:
        fig_b1 = go.Figure()
        # Stacked Bars
        fig_b1.add_trace(go.Bar(x=df_deployers_overtime['Date'], y=df_deployers_overtime['New Deployers'], name="New Deployers", marker_color="#fc9047"))
        fig_b1.add_trace(go.Bar(x=df_deployers_overtime['Date'], y=df_deployers_overtime['Returning Deployers'], name="Returning Deployers", marker_color="#858dff"))
        fig_b1.add_trace(go.Scatter(x=df_deployers_overtime['Date'], y=df_deployers_overtime['Total Deployers'], name="Total Deployers", mode="lines", line=dict(color="black", width=2)))
        fig_b1.update_layout(barmode="stack", title="Number of Token Deployers Over Time", yaxis=dict(title="Address count"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5))
        st.plotly_chart(fig_b1, use_container_width=True)

    with col2:
        fig_stacked_tokens = px.bar(df_deployed_tokens, x="Date", y="Number of Tokens", color="Token Type", title="Number of Tokens Deployed Over Time", color_discrete_map=color_map)
        fig_stacked_tokens.update_layout(barmode="stack", yaxis_title="Number of Tokens", xaxis_title="", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5, title=""))
        st.plotly_chart(fig_stacked_tokens, use_container_width=True)

    # --- Row 3,4 -------------------------------------------------------------------------------------------------------------------------------------------------------------------
    @st.cache_data
    def load_deploy_fee_stats_overtime(timeframe, start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        with table1 as (
    SELECT created_at, data:interchain_token_deployment_started:tokenId as token, 
    data:call:transaction:from as deployer, COALESCE(CASE 
            WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
              OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
            THEN NULL
            WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
              AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
            THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
            ELSE NULL
          END, CASE 
            WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
            WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
            ELSE NULL
          END) AS fee,
          LOWER(data:call.chain::STRING) AS "Deployed Chain"
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed' AND simplified_status = 'received' AND (
    data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
    or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
    ) AND data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
    and created_at::date>='{start_str}' and created_at::date<='{end_str}')

    select date_trunc('{timeframe}',created_at) as "Date", "Deployed Chain", round(sum(fee),2) as "Total Gas Fees",
    round(avg(fee),3) as "Avg Gas Fee"
    from table1
    group by 1, 2
    order by 1

        """

        df = pd.read_sql(query, conn)
        return df

    # --- Row 4 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
    @st.cache_data
    def load_gas_fee_stats(start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        with table1 as (
    SELECT created_at, data:interchain_token_deployment_started:tokenId as token, 
    data:call:transaction:from as deployer, COALESCE(CASE 
            WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
              OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
            THEN NULL
            WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
              AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
            THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
            ELSE NULL
          END, CASE 
            WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
            WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
            ELSE NULL
          END) AS fee
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed' AND simplified_status = 'received' AND (
    data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
    or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
    ) AND data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
    and created_at::date>='{start_str}' and created_at::date<='{end_str}')

    select round(avg(fee),3) as "Avg Gas Fee", round(median(fee),3) as "Median Gas Fee", round(max(fee)) as "Max Gas Fee"
    from table1

        """

        df = pd.read_sql(query, conn)
        return df

    # --- Row 5 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------
    @st.cache_data
    def load_avg_median_fee_stats(timeframe, start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        with table1 as (
    SELECT created_at, data:interchain_token_deployment_started:tokenId as token, 
    data:call:transaction:from as deployer, COALESCE(CASE 
            WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
              OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
            THEN NULL
            WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
              AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
            THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
            ELSE NULL
          END, CASE 
            WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
            WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
            ELSE NULL
          END) AS fee
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed' AND simplified_status = 'received' AND (
    data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
    or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
    ) AND data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
    and created_at::date>='{start_str}' and created_at::date<='{end_str}')

    select date_trunc('{timeframe}',created_at) as "Date", round(avg(fee),3) as "Avg Gas Fee", round(median(fee),3) as "Median Gas Fee"
    from table1
    group by 1
    order by 1

        """

        df = pd.read_sql(query, conn)
        return df

    # === Load Data: Row 3,4,5 ==================================================================
    df_deploy_fee_stats_overtime = load_deploy_fee_stats_overtime(timeframe, start_date, end_date)
    df_avg_median_fee_stats = load_avg_median_fee_stats(timeframe, start_date, end_date)
    df_gas_fee_stats = load_gas_fee_stats(start_date, end_date)
    # === Charts: Row 3 =====================================================================

    col1, col2 = st.columns(2)

    with col1:
        fig_b1 = go.Figure()
        # Stacked Bars
        fig_stacked_fee_chain = px.bar(df_deploy_fee_stats_overtime, x="Date", y="Total Gas Fees", color="Deployed Chain", 
                                    title="Amount of Fees Paid Based on the Deployed Chain Over Time")
        fig_stacked_fee_chain.update_layout(barmode="stack", yaxis_title="$USD", xaxis_title="", legend=dict(title=""))
        st.plotly_chart(fig_stacked_fee_chain, use_container_width=True)

    with col2:
        df_norm = df_deploy_fee_stats_overtime.copy()
        df_norm['total_per_date'] = df_norm.groupby('Date')['Total Gas Fees'].transform('sum')
        df_norm['normalized'] = df_norm['Total Gas Fees'] / df_norm['total_per_date']
        fig_norm_stacked_fee_chain = px.bar(df_norm, x='Date', y='normalized', color='Deployed Chain', title="Share of Fees Paid Based on the Deployed Chain Over Time",
                                         text=df_norm['Total Gas Fees'].astype(str))
        fig_norm_stacked_fee_chain.update_layout(barmode='stack', xaxis_title="", yaxis_title="%", yaxis=dict(tickformat='%'), legend=dict(title=""))
        fig_norm_stacked_fee_chain.update_traces(textposition='inside')
        st.plotly_chart(fig_norm_stacked_fee_chain, use_container_width=True)

    # === KPIs: Row 4 ======================================================
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
        st.markdown(card_style.format(label="Avg Gas Fee", value=f"📊${df_gas_fee_stats['Avg Gas Fee'][0]:,}"), unsafe_allow_html=True)
    with col2:
        st.markdown(card_style.format(label="Median Gas Fee", value=f"📋${df_gas_fee_stats['Median Gas Fee'][0]:,}"), unsafe_allow_html=True)
    with col3:
        st.markdown(card_style.format(label="Max Gas Fee", value=f"📈${df_gas_fee_stats['Max Gas Fee'][0]:,}"), unsafe_allow_html=True)

    # === Charts: Row 5 ======================================================
    col1, col2 = st.columns(2)

    with col1:
        fig_line_gas = px.line(df_deploy_fee_stats_overtime, x="Date", y="Avg Gas Fee", color="Deployed Chain", title="Avg Gas Fee by Chain Over Time")
        fig_line_gas.update_layout(yaxis_title="$USD", xaxis_title="", legend=dict(title=""))
        st.plotly_chart(fig_line_gas, use_container_width=True)

    with col2:
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=df_avg_median_fee_stats['Date'], y=df_avg_median_fee_stats['Avg Gas Fee'], name="Avg Gas Fee", mode="lines", 
                                  yaxis="y1", line=dict(color="#fa9550")))
        fig2.add_trace(go.Scatter(x=df_avg_median_fee_stats['Date'], y=df_avg_median_fee_stats['Median Gas Fee'], name="Median Gas Fee", mode="lines", 
                                  yaxis="y2", line=dict(color="#858dff")))
        fig2.update_layout(title="Average & Median Fee For Token Deployment", yaxis=dict(title="$USD"), yaxis2=dict(title="$USD", overlaying="y", side="right"), xaxis=dict(title=""),
            barmode="group", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
        st.plotly_chart(fig2, use_container_width=True)

    # --- Row 6 -----------------------------------------------------------------------------------------------------------------------------------------------------------------
    @st.cache_data
    def load_deploy_stats_by_chain(start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        with table1 as (
    SELECT created_at, data:interchain_token_deployment_started:tokenId as token, 
    data:call:transaction:from as deployer, COALESCE(CASE 
            WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
              OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
            THEN NULL
            WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
              AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
            THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
            ELSE NULL
          END, CASE 
            WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
            WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
            ELSE NULL
          END) AS fee,
          LOWER(data:call.chain::STRING) AS "Deployed Chain"
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed' AND simplified_status = 'received' AND (
    data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
    or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
    ) AND data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
    and created_at::date>='{start_str}' and created_at::date<='{end_str}')

    select "Deployed Chain", round(sum(fee),2) as "Total Gas Fees", count(distinct token) as "Number of Tokens"
    from table1
    group by 1
    order by 2 desc 

        """

        df = pd.read_sql(query, conn)
        return df

    # === Load Data: Row 6 =======================================================================================
    df_deploy_stats_by_chain = load_deploy_stats_by_chain(start_date, end_date)
    # === Charts: Row 6 ==========================================================================================
    col1, col2 = st.columns(2)

    fig1 = px.pie(df_deploy_stats_by_chain, values="Number of Tokens", names="Deployed Chain", title="Number of Tokens Deployed By Chain")
    fig1.update_traces(textinfo="percent+label", textposition="inside", automargin=True)

    fig2 = px.pie(df_deploy_stats_by_chain, values="Total Gas Fees", names="Deployed Chain", title="Total Gas Fee By Deployed Chain")
    fig2.update_traces(textinfo="percent+label", textposition="inside", automargin=True)

    # display charts
    col1.plotly_chart(fig1, use_container_width=True)
    col2.plotly_chart(fig2, use_container_width=True)

    # --- Row 7 --------------------------------------------------------------------------------------------------------------------------------------------------------------------
    @st.cache_data
    def load_list_tokens(start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        with tab3 as (with tab1 as (SELECT data:interchain_token_deployment_started:tokenName as token_name,
    data:interchain_token_deployment_started:tokenSymbol as symbol,
    call:chain as chain
    FROM axelar.axelscan.fact_gmp
    where data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
    and (data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' 
    or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%')
    and status='executed'
    and event='ContractCall'
    and simplified_status='received'
    and (call:receipt:logs[0]:address not ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' and 
    call:receipt:logs[0]:address not ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%')
    and created_at::date between '{start_str}' and '{end_str}'),

    tab2 as (SELECT data:interchain_token_deployment_started:tokenName as token_name,
    data:interchain_token_deployment_started:tokenSymbol as symbol,
    data:call:returnValues:destinationChain as chain

    FROM axelar.axelscan.fact_gmp

    where data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
    and (data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' 
    or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%')
    and status='executed'
    and simplified_status='received'
    and created_at::date between '{start_str}' and '{end_str}')

    select * from tab1 union all 
    select * from tab2)

    select token_name as "Token Name", symbol as "Token Symbol", count(distinct lower(chain)) as "Chains Count" 
    from tab3
    group by 1,2
    having count(distinct lower(chain))>1
    order by 3 desc 

        """

        df = pd.read_sql(query, conn)
        return df

    # --- Row 8 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
    @st.cache_data
    def load_tracking_tokens(start_date, end_date):

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"""
        SELECT created_at as "Date", data:call:transaction:from as "Deployer", data:interchain_token_deployment_started:tokenName as "Token Name",
    data:interchain_token_deployment_started:tokenSymbol as "Token Symbol", case 
    when (call:receipt:logs[0]:address ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' or 
    call:receipt:logs[0]:address ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%') then 'Existing Tokens'
    else 'Newly Minted Token' end as "Token Type", call:chain as "Deployed Chain",
    data:call:returnValues:destinationChain as "Registered Chain",
    data:interchain_token_deployment_started:tokenId as "Token ID", COALESCE(CASE 
            WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
              OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
            THEN NULL
            WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
              AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
            THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
            ELSE NULL
          END, CASE 
            WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
            WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
            ELSE NULL
          END) AS "Fee"
    FROM axelar.axelscan.fact_gmp
    where data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
    and (data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' 
    or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%')
    and status='executed'
    and simplified_status='received'
    and created_at::date between '{start_str}' and '{end_str}'
    order by 1 desc 

        """

        df = pd.read_sql(query, conn)
        return df

    # === Load Data: Rows 7,8 ==============================================
    df_list_tokens = load_list_tokens(start_date, end_date)
    df_tracking_tokens = load_tracking_tokens(start_date, end_date)
    # === Tables 7,8 =======================================================
    st.subheader("📑List of ITS Tokens By Number of Registered Chains (Tokens on 2+ chains)")
    df_display_token_chain = df_list_tokens.copy()
    df_display_token_chain.index = df_display_token_chain.index + 1
    df_display_token_chain = df_display_token_chain.applymap(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)
    st.dataframe(df_display_token_chain, use_container_width=True)

    st.subheader("🎯Tracking of Token Deployments")
    df_display = df_tracking_tokens.copy()
    df_display.index = df_display.index + 1
    df_display = df_display.applymap(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)
    st.dataframe(df_display, use_container_width=True)
