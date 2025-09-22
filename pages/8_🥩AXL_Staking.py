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
start_date = st.date_input("Start Date", value=pd.to_datetime("2022-08-01"))
end_date = st.date_input("End Date", value=pd.to_datetime("2025-07-30"))

# --- Query Functions -----------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_share_of_staked_tokens(start_date, end_date):
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
              AND block_timestamp::date >= '{start_date}'
              AND block_timestamp::date <= '{end_date}'
            GROUP BY 1
        )
        SELECT 
            (cumulative_delegate_amount + cumulative_undelegate_amount) / 1008585017 * 100 AS share_of_staked_tokens
        FROM delegate a
        LEFT OUTER JOIN undelegate b
          ON a.monthly = b.monthly
        WHERE a.monthly >= '{start_date}'
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

# --- Row3: All-Time Delegate KPIs ---
@st.cache_data
def load_delegate_kpis(start_date, end_date):
    query = f"""
        SELECT 
            ROUND(SUM(amount/POW(10,6)), 2) AS amount,
            COUNT(DISTINCT tx_id) AS txns,
            COUNT(DISTINCT DELEGATOR_ADDRESS) AS user,
            ROUND(AVG(amount/POW(10,6)), 2) AS avg_amount
        FROM axelar.gov.fact_staking
        WHERE action = 'delegate'
          AND block_timestamp::date >= '{start_date}'
          AND block_timestamp::date <= '{end_date}'
    """
    return pd.read_sql(query, conn)

# --- Row4: Current Net Staked --------
@st.cache_data
def load_current_net_staked(start_date, end_date):
    query = f"""
        WITH delegate AS (
            SELECT 
                TRUNC(block_timestamp,'month') AS monthly, 
                SUM(amount/POW(10,6)) AS delegate_amount,
                SUM(SUM(amount/POW(10,6))) OVER (ORDER BY TRUNC(block_timestamp,'month') ASC) AS cumulative_delegate_amount
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
                SUM(SUM(amount/POW(10,6)) * -1) OVER (ORDER BY TRUNC(block_timestamp,'month') ASC) AS cumulative_undelegate_amount
            FROM axelar.gov.fact_staking
            WHERE action = 'undelegate'
              AND block_timestamp::date >= '{start_date}'
              AND block_timestamp::date <= '{end_date}'
            GROUP BY 1
        )
        SELECT 
            ROUND((cumulative_delegate_amount + cumulative_undelegate_amount), 1) AS Net
        FROM delegate a 
        LEFT JOIN undelegate b ON a.monthly = b.monthly
        WHERE a.monthly >= '{start_date}'
        ORDER BY a.monthly DESC
        LIMIT 1
    """
    df = pd.read_sql(query, conn)
    if not df.empty:
        return df["NET"].iloc[0]
    else:
        return None

# --- Row5,6: Monthly Delegation Data --------------
@st.cache_data
def load_monthly_delegation_data(start_date, end_date):
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
            ROUND(delegate_amount,1) AS "Delegate Amount",
            ROUND(undelegate_amount,1) AS "Undelegate Amount",
            cumulative_delegate_amount,
            cumulative_undelegate_amount,
            delegate_tx AS "Delegate Txns",
            undelegate_tx AS "Undelegate Txns",
            delegate_user AS "Delegators",
            undelegate_user AS "Undelegators",
            ROUND((cumulative_delegate_amount + cumulative_undelegate_amount),1) AS "Net Delegated Amount"
        FROM delegate a 
        LEFT JOIN undelegate b ON a.monthly = b.monthly 
        WHERE a.monthly >= '{start_date}'
        ORDER BY a.monthly ASC
    """
    df = pd.read_sql(query, conn)
    if not df.empty:
        df['monthly'] = pd.to_datetime(df['MONTHLY'])
    return df

# --- Row7: Users, Txns & Amount By Action ------------------------------------------------------------
@st.cache_data
def load_action_summary_by_type(start_date, end_date):
    query = f"""
        SELECT 'Delegate' AS "Type", 
               ROUND(SUM(amount/POW(10,6)),1) AS "Amount",
               COUNT(DISTINCT tx_id) AS "Txns",
               COUNT(DISTINCT DELEGATOR_ADDRESS) AS "Users"
        FROM axelar.gov.fact_staking
        WHERE action = 'delegate'
          AND block_timestamp::date >= '{start_date}'
          AND block_timestamp::date <= '{end_date}'
        GROUP BY 1
        UNION
        SELECT 'Undelegate' AS "Type",
               ROUND(SUM(amount/POW(10,6)),1) AS "Amount",
               COUNT(DISTINCT tx_id) AS "Txns",
               COUNT(DISTINCT DELEGATOR_ADDRESS) AS "Users"
        FROM axelar.gov.fact_staking
        WHERE action = 'undelegate'
          AND block_timestamp::date >= '{start_date}'
          AND block_timestamp::date <= '{end_date}'
        GROUP BY 1
    """
    return pd.read_sql(query, conn)
# -- Row8 -----------------------------------------------------------
@st.cache_data
def load_current_number_of_delegators(start_date, end_date):
    query = f"""
        WITH date_start AS (
            WITH dates AS (
                SELECT CAST('2022-02-10' AS DATE) AS start_date 
                UNION ALL 
                SELECT DATEADD(day, 1, start_date)
                FROM dates
                WHERE start_date < CURRENT_DATE()
            )
            SELECT DATE_TRUNC('day', start_date) AS start_date
            FROM dates
        ),
        axl_stakers_balance_change AS (
            SELECT *
            FROM (
                SELECT DATE_TRUNC('day', block_timestamp) AS date,
                       DELEGATOR_ADDRESS AS user,
                       SUM(amount)/1e6 AS balance_change
                FROM (
                    SELECT block_timestamp, DELEGATOR_ADDRESS, -1 * amount AS amount, tx_id
                    FROM axelar.gov.fact_staking
                    WHERE action = 'undelegate' AND tx_succeeded = TRUE
                    UNION ALL
                    SELECT block_timestamp, DELEGATOR_ADDRESS, amount, tx_id
                    FROM axelar.gov.fact_staking
                    WHERE action = 'delegate' AND tx_succeeded = TRUE
                )
                GROUP BY 1,2
            )
        ),
        axl_stakers_historic_holders AS (
            SELECT user
            FROM axl_stakers_balance_change
            GROUP BY 1
        ),
        user_dates AS (
            SELECT start_date, user
            FROM date_start, axl_stakers_historic_holders
        ),
        users_balance AS (
            SELECT start_date AS "Date", user,
                   LAG(balance_raw) IGNORE NULLS OVER (PARTITION BY user ORDER BY start_date) AS balance_lag,
                   IFNULL(balance_raw, balance_lag) AS balance
            FROM (
                SELECT start_date, a.user, balance_change,
                       SUM(balance_change) OVER (PARTITION BY a.user ORDER BY start_date) AS balance_raw
                FROM user_dates a
                LEFT JOIN axl_stakers_balance_change b
                ON date = start_date AND a.user = b.user
            )
        )
        SELECT "Date", COUNT(DISTINCT user) AS "Users"
        FROM users_balance
        WHERE balance >= 0.001 AND balance IS NOT NULL
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 1
    """
    df = pd.read_sql(query, conn)
    if not df.empty:
        return int(df["Users"].iloc[0])
    else:
        return None

# --- Row9: Top Delegators -----------------------------------------------------------------------------
@st.cache_data
def load_top_delegators(start_date, end_date):
    query = f"""
        WITH delegate AS (
            SELECT delegator_address,
                   ROUND(SUM(amount/POW(10,6)),1) AS delegate_amount,
                   COUNT(DISTINCT tx_id) AS delegate_txns,
                   ROUND(AVG(amount/POW(10,6)),1) AS avg_delegate_amount
            FROM axelar.gov.fact_staking
            WHERE action = 'delegate'
              AND block_timestamp::date >= '{start_date}'
              AND block_timestamp::date <= '{end_date}'
            GROUP BY 1
        ),
        undelegate AS (
            SELECT delegator_address,
                   ROUND(SUM(amount/POW(10,6)),1) AS undelegate_amount,
                   COUNT(DISTINCT tx_id) AS undelegate_txns,
                   ROUND(AVG(amount/POW(10,6)),1) AS avg_undelegate_amount
            FROM axelar.gov.fact_staking
            WHERE action = 'undelegate'
              AND block_timestamp::date >= '{start_date}'
              AND block_timestamp::date <= '{end_date}'
            GROUP BY 1
        )
        SELECT a.delegator_address AS "Delegator Address",
               delegate_amount AS "Delegate Amount",
               IFNULL(undelegate_amount,0) AS "Undelegate Amount",
               delegate_amount - IFNULL(undelegate_amount,0) AS "Net Delegated",
               delegate_txns AS "Delegate Txns",
               IFNULL(undelegate_txns,0) AS "Undelegate Txns",
               avg_delegate_amount AS "Avg Delegate Txns",
               IFNULL(avg_undelegate_amount,0) AS "Avg Undelegate Txns"
        FROM delegate a
        LEFT JOIN undelegate b
          ON a.delegator_address = b.delegator_address
        ORDER BY 4 DESC
        LIMIT 1000
    """
    df = pd.read_sql(query, conn)
    if not df.empty:
        df.index = df.index + 1  
        return df
    else:
        return pd.DataFrame()

# --- Row10: Users Breakdown -----------------
@st.cache_data
def load_users_breakdown(start_date, end_date):
    query = f"""
        WITH final AS (
            SELECT 'Delegate' AS "Type",
                   DELEGATOR_ADDRESS,
                   SUM(amount/POW(10,6)) AS amount,
                   COUNT(DISTINCT tx_id) AS txns,
                   COUNT(DISTINCT DELEGATOR_ADDRESS) AS user,
                   AVG(amount/POW(10,6)) AS avg_amount
            FROM axelar.gov.fact_staking
            WHERE action = 'delegate'
              AND block_timestamp::date >= '{start_date}'
              AND block_timestamp::date <= '{end_date}'
            GROUP BY 1,2
            UNION
            SELECT 'Undelegate' AS "Type",
                   DELEGATOR_ADDRESS,
                   SUM(amount/POW(10,6)) AS amount,
                   COUNT(DISTINCT tx_id) AS txns,
                   COUNT(DISTINCT DELEGATOR_ADDRESS) AS user,
                   AVG(amount/POW(10,6)) AS avg_amount
            FROM axelar.gov.fact_staking
            WHERE action = 'undelegate'
              AND block_timestamp::date >= '{start_date}'
              AND block_timestamp::date <= '{end_date}'
            GROUP BY 1,2
        )
        SELECT COUNT(DISTINCT DELEGATOR_ADDRESS) AS "Users Count",
               "Type",
               CASE
                   WHEN amount <= 10 THEN '<= 10 Axl'
                   WHEN amount <= 100 THEN '10-100 Axl'
                   WHEN amount <= 1000 THEN '100-1k Axl'
                   WHEN amount <= 10000 THEN '1k-10k Axl'
                   WHEN amount <= 100000 THEN '10k-100k Axl'
                   WHEN amount <= 1000000 THEN '100k-1m Axl'
                   WHEN amount > 1000000 THEN '> 1m Axl'
               END AS "Category"
        FROM final
        GROUP BY 2,3
    """
    return pd.read_sql(query, conn)

# --- Row11: New Delegators KPIs ---------------------------
@st.cache_data
def load_new_delegators():
    query = """
        WITH new AS (
            SELECT MIN(block_timestamp) AS daily,
                   delegator_address
            FROM axelar.gov.fact_staking
            WHERE action = 'delegate'
            GROUP BY 2
        ),
        final AS (
            SELECT TRUNC(daily,'day') AS day,
                   COUNT(DISTINCT delegator_address) AS new_staker,
                   SUM(COUNT(DISTINCT delegator_address)) OVER (ORDER BY TRUNC(daily,'day') ASC) AS cumulative_new_staker
            FROM new
            GROUP BY 1
        )
        SELECT SUM(new_staker) AS "Total Number of New Delegators",
               ROUND(AVG(new_staker)) AS "Avg Number of Daily Delegators"
        FROM final
        WHERE day >= '2025-01-01'
    """
    return pd.read_sql(query, conn)

# --- Row12: Monthly New Delegators ----------------------------------------------------------------------
@st.cache_data
def load_monthly_new_delegators(start_date, end_date):
    query = f"""
        WITH new AS (
            SELECT MIN(block_timestamp) AS daily,
                   delegator_address
            FROM axelar.gov.fact_staking
            WHERE action = 'delegate'
            GROUP BY 2
        )
        SELECT TRUNC(daily, 'month') AS "Month",
               COUNT(DISTINCT delegator_address) AS "New Delegators",
               SUM(COUNT(DISTINCT delegator_address)) OVER (ORDER BY TRUNC(daily, 'month') ASC) AS "Cumulative New Delegators"
        FROM new
        WHERE daily::date >= '{start_date}' AND daily::date <= '{end_date}'
        GROUP BY 1
        ORDER BY 1
    """
    return pd.read_sql(query, conn)

# --- Row13 -----------------------------------------
@st.cache_data
def load_daily_share_delegated_amount():
    query = """
        WITH new AS (
            SELECT MIN(block_timestamp) AS daily, DELEGATOR_ADDRESS
            FROM axelar.gov.fact_staking
            WHERE action = 'delegate'
            GROUP BY 2
        ),
        final AS (
            SELECT DISTINCT DELEGATOR_ADDRESS
            FROM new
            WHERE daily >= CURRENT_DATE - 90
        )
        SELECT DATE(block_timestamp) AS "Date",
               CASE WHEN delegator_address IN (SELECT delegator_address FROM final) THEN 'New Stakers'
                    ELSE 'Active Stakers' END AS "Type",
               SUM(amount/POW(10,6)) AS "Delegated Amount"
        FROM axelar.gov.fact_staking
        WHERE action = 'delegate'
          AND TX_SUCCEEDED = TRUE
          AND block_timestamp::date >= CURRENT_DATE - 61
        GROUP BY 1,2
        ORDER BY 1 ASC
    """
    return pd.read_sql(query, conn)

@st.cache_data
def load_share_amount():
    query = """
        WITH new AS (
            SELECT MIN(block_timestamp) AS daily, delegator_address
            FROM axelar.gov.fact_staking
            WHERE action = 'delegate'
            GROUP BY 2
        ),
        final AS (
            SELECT DISTINCT delegator_address
            FROM new
            WHERE daily >= CURRENT_DATE - 90
        )
        SELECT CASE WHEN delegator_address IN (SELECT delegator_address FROM final) THEN 'New Stakers'
                    ELSE 'Active Stakers' END AS "Type",
               ROUND(SUM(amount/POW(10,6))) AS "Delegated Amount"
        FROM axelar.gov.fact_staking
        WHERE action = 'delegate'
          AND TX_SUCCEEDED = TRUE
          AND block_timestamp::date >= CURRENT_DATE - 61
        GROUP BY 1
    """
    return pd.read_sql(query, conn)

# --- Row 14,15 --------------------------------------------------------------------------------------------------
@st.cache_data
def load_monthly_new_validators(start_date, end_date):
    query = f"""
        WITH validator AS (
            SELECT MIN(block_timestamp) AS date,
                   validator_address,
                   SUM(amount/POW(10,6)) AS delegate_amount,
                   COUNT(DISTINCT tx_id) AS delegate_tx,
                   COUNT(DISTINCT DELEGATOR_ADDRESS) AS delegate_user,
                   AVG(amount/POW(10,6)) AS avg_delegate_amount 
            FROM axelar.gov.fact_staking
            GROUP BY 2
        )
        SELECT TRUNC(date,'month') AS "Month",
               COUNT(DISTINCT validator_address) AS "New Validators",
               SUM(COUNT(DISTINCT validator_address)) OVER (ORDER BY TRUNC(date,'month') ASC) AS "Cumulative New Validators",
               75 AS "Active Validators"
        FROM validator
        WHERE date >= '{start_date}'
          AND date <= '{end_date}'
        GROUP BY 1
        ORDER BY 1
    """
    return pd.read_sql(query, conn)

# --- Row16: Redelegations -------------------------------------------------------------------
def get_redelegate_data():
    query = """
    with validators as (
        select ifnull(b.label, a.validator_address) as source, 
               ifnull(c.label, a.validator_address) as to_validator,
               amount/pow(10,6) as amt,
               tx_id
        from axelar.gov.fact_staking a 
        left outer join axelar.gov.fact_validators b 
            on a.REDELEGATE_SOURCE_VALIDATOR_ADDRESS = b.address
        left outer join axelar.gov.fact_validators c 
            on a.VALIDATOR_ADDRESS = c.address
        where action = 'redelegate'
          and TX_SUCCEEDED = 'TRUE'
    )
    select concat(source, '->', to_validator) as "Validator",
           sum(amt) as "Redelegate Amount", 
           avg(amt) as "Avg Amount",
           count(DISTINCT tx_id) as "Transactions" 
    from validators 
    group by 1
    order by 2 desc 
    limit 10
    """
    return pd.read_sql(query, conn)

# --- Row 17 --------------------------
def get_net_delegated_per_validator():
    query = """
    with delegate as (
        select validator_address, 
               tx_id,
               amount/pow(10,6) as delegate_amt,
               delegator_address
        from axelar.gov.fact_staking
        where action = 'delegate'
          and TX_SUCCEEDED = 'TRUE'
        UNION
        select validator_address, 
               tx_id,
               amount/pow(10,6) as delegate_amt,
               delegator_address
        from axelar.gov.fact_staking
        where action = 'redelegate'
          and TX_SUCCEEDED = 'TRUE'
    ),
    undelegate as (
        select validator_address, 
               tx_id,
               amount/pow(10,6) as undelegate_amt,
               delegator_address
        from axelar.gov.fact_staking
        where action = 'undelegate'
          and TX_SUCCEEDED = 'TRUE'
          and block_timestamp::date >= '2022-08-01'
        UNION
        select REDELEGATE_SOURCE_VALIDATOR_ADDRESS as validator_address, 
               tx_id,
               amount/pow(10,6) as undelegate_amt,
               delegator_address
        from axelar.gov.fact_staking
        where action = 'redelegate'
          and TX_SUCCEEDED = 'TRUE'
          and block_timestamp::date >= '2022-08-01'
    ),
    delegation as (
        select validator_address,
               sum(delegate_amt) as delegate_amounts
        from delegate
        group by 1
    ),
    undelegation as (
        select validator_address,
               sum(undelegate_amt) as undelegate_amounts
        from undelegate
        group by 1
    )
    select ifnull(label, a.validator_address) as "Validator",
           round(delegate_amounts,1) as "Delegate Amount",
           round(ifnull(undelegate_amounts,0),1) as "Undelegate Amount",
           round(("Delegate Amount" - "Undelegate Amount"),1) as "Net Delegate Amount" 
    from delegation a 
    left outer join undelegation b on a.validator_address = b.validator_address
    left outer join axelar.gov.fact_validators c on a.validator_address = c.address
    order by 4 desc 
    limit 75
    """
    return pd.read_sql(query, conn)
   
# --- Load Data ---------------------------------------------------------------------------------------------------------------------------------------------------------------
share_of_staked_tokens = load_share_of_staked_tokens(start_date, end_date)
monthly_share_df = load_monthly_share_data(start_date, end_date)
delegate_kpis_df = load_delegate_kpis(start_date, end_date)
current_net_staked = load_current_net_staked(start_date, end_date)
monthly_data = load_monthly_delegation_data(start_date, end_date)
action_summary2 = load_action_summary_by_type(start_date, end_date)
current_delegators = load_current_number_of_delegators(start_date, end_date)
top_delegators_df = load_top_delegators(start_date, end_date)
users_breakdown_df = load_users_breakdown(start_date, end_date)
new_delegators_df = load_new_delegators()
monthly_new_delegators = load_monthly_new_delegators(start_date, end_date)
daily_share = load_daily_share_delegated_amount()
share_amount = load_share_amount()
monthly_validators = load_monthly_new_validators(start_date, end_date)
redelegate_data = get_redelegate_data()
net_delegate_data = get_net_delegated_per_validator()

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

# --- Row 3: KPIs -------------------------------
st.markdown(
    """
    <div style="background-color:#fc0060; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Overview</h2>
    </div>
    """,
    unsafe_allow_html=True
)

if not delegate_kpis_df.empty:
    amount = delegate_kpis_df["AMOUNT"].iloc[0]
    avg_amount = delegate_kpis_df["AVG_AMOUNT"].iloc[0]
    txns = delegate_kpis_df["TXNS"].iloc[0]
    user = delegate_kpis_df["USER"].iloc[0]

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(label="Total Delegated Amount (All Time)", value=f"{amount:,.2f} AXL")
        st.caption("(Exclude Undelegate Amount)")

    with col2:
        st.metric(label="Average Delegate Amount", value=f"{avg_amount:,.2f} AXL")

    with col3:
        st.metric(label="Total Delegate Transactions", value=f"{txns:,}")
        st.caption("(All Time)")

    with col4:
        st.metric(label="Total Delegators", value=f"{user:,}")
        st.caption("Cumulative count of all users who have ever delegated AXL")
else:
    st.warning("No delegate KPI data available for the selected period.")

# --- Row 4: Single KPI -----------------------------
if current_net_staked is not None:
    st.markdown(
        f"""
        <div style="text-align: center; padding: 40px; background-color: #f8f9fa; border-radius: 15px; margin: 20px 0;">
            <h2 style="font-size: 32px; margin-bottom: 10px;">Current Net Staked</h2>
            <p style="font-size: 48px; font-weight: bold; color: #2e7d32;">{current_net_staked:,.1f} AXL</p>
        </div>
        """,
        unsafe_allow_html=True
    )
else:
    st.warning("No data available for Current Net Staked in the selected period.")
    
if not monthly_data.empty:
# --- Row 5: Combined Delegate & Undelegate + Net --------------------------
    fig1 = go.Figure()
    fig1.add_bar(x=monthly_data['monthly'], y=monthly_data['Delegate Amount'], name='Delegate Amount', marker_color='blue', yaxis='y1')
    fig1.add_bar(x=monthly_data['monthly'], y=monthly_data['Undelegate Amount'], name='Undelegate Amount', marker_color='orange', yaxis='y1')
    fig1.add_trace(go.Scatter(x=monthly_data['monthly'], y=monthly_data['Net Delegated Amount'],
                              name='Net Delegated Amount', mode='lines+markers', line=dict(color='yellow', width=2), yaxis='y2'))
    fig1.update_layout(
        title="Monthly Delegate and Undelegate Amount + Net (AXL)",
        barmode='group',
        yaxis=dict(title="$AXL", side='left'),
        yaxis2=dict(title="$AXL", overlaying='y', side='right'),
        legend=dict(x=0, y=1.1, orientation='h'),
        height=500
    )
    st.plotly_chart(fig1, use_container_width=True)

    # --- Row 6: Two Side-by-Side Charts ---------------
    col1, col2 = st.columns(2)

    # Monthly Number of Users
    with col1:
        fig2 = go.Figure()
        fig2.add_bar(x=monthly_data['monthly'], y=monthly_data['Delegators'], name='Delegators', marker_color='blue')
        fig2.add_bar(x=monthly_data['monthly'], y=monthly_data['Undelegators'], name='Undelegators', marker_color='orange')
        fig2.update_layout(
            title="Monthly Number of Users",
            barmode='group',
            yaxis_title="Number of Users",
            legend=dict(x=0, y=1.1, orientation='h'),
            height=400
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Monthly Number of Transactions
    with col2:
        fig3 = go.Figure()
        fig3.add_bar(x=monthly_data['monthly'], y=monthly_data['Delegate Txns'], name='Delegate Txns', marker_color='blue')
        fig3.add_bar(x=monthly_data['monthly'], y=monthly_data['Undelegate Txns'], name='Undelegate Txns', marker_color='orange')
        fig3.update_layout(
            title="Monthly Number of Transactions",
            barmode='group',
            yaxis_title="Number of Transactions",
            legend=dict(x=0, y=1.1, orientation='h'),
            height=400
        )
        st.plotly_chart(fig3, use_container_width=True)
else:
    st.warning("No data available for Monthly Delegation details in the selected period.")

# --- Row 7: Three Charts -------------------------------------------------------------------------------------------
if not action_summary2.empty:
    col1, col2, col3 = st.columns(3)

    # Chart 1: Number of Users By Action
    with col1:
        fig1 = go.Figure()
        fig1.add_bar(
            x=action_summary2["Type"],
            y=action_summary2["Users"],
            text=action_summary2["Users"],
            textposition="outside",
            marker_color=["#1f77b4", "#ff7f0e"]
        )
        fig1.update_layout(
            title="Number of Users By Action",
            yaxis_title="Users",
            height=400
        )
        st.plotly_chart(fig1, use_container_width=True)

    # Chart 2: Number of Transactions By Action (Donut)
    with col2:
        fig2 = go.Figure(data=[
            go.Pie(
                labels=action_summary2["Type"],
                values=action_summary2["Txns"],
                hole=0.4,
                textinfo="label+percent",
                hovertemplate="%{label}: %{value} Txns"
               
            )
        ])
        fig2.update_layout(
            title="Number of Transactions By Action",
            height=400,
            legend=dict(x=1,y=0.5,xanchor="left",yanchor="middle",orientation="v")
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Chart 3: Amount of Transactions By Action (Donut)
    with col3:
        fig3 = go.Figure(data=[
            go.Pie(
                labels=action_summary2["Type"],
                values=action_summary2["Amount"],
                hole=0.4,
                textinfo="label+percent",
                hovertemplate="%{label}: %{value} AXL"
            )
        ])
        fig3.update_layout(
            title="Amount of Transactions By Action",
            height=400,
            legend=dict(x=1,y=0.5,xanchor="left",yanchor="middle",orientation="v")
        )
        st.plotly_chart(fig3, use_container_width=True)

else:
    st.warning("No data available for the selected period.")

# --- Row8: Single KPI -------------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="background-color:#fc0060; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Users</h2>
    </div>
    """,
    unsafe_allow_html=True
)

if current_delegators is not None:
    st.markdown(
        f"""
        <div style="text-align: center; padding: 30px; background-color: #f8f9fa; border-radius: 15px; margin: 20px 0;">
            <h3 style="font-size: 28px; margin-bottom: 10px;">Current Number of Delegators</h3>
            <p style="font-size: 40px; font-weight: bold; color: #2e7d32;">{current_delegators:,}</p>
            <p style="font-size: 14px; color: #555;">Number of Users with AXL Currently Staked</p>
        </div>
        """,
        unsafe_allow_html=True
    )
else:
    st.warning("No data available for Current Number of Delegators in the selected period.")

# --- Row9: Display Table -----------------------------------------------
if not top_delegators_df.empty:
    
    top_delegators_df = top_delegators_df.applymap(
        lambda x: '{:,.0f}'.format(x) if isinstance(x, (int, float)) else x
    )

    def highlight_top3(row):
        color = ''
        if row.name == 1:   # -- rank1-gold
            color = 'background-color: gold;'
        elif row.name == 2: # -- rank2 silver
            color = 'background-color: silver;'
        elif row.name == 3: # -- rank3-bronze
            color = 'background-color: #cd7f32;'
        return [color] * len(row)

    styled_table = top_delegators_df.style.apply(highlight_top3, axis=1)

    st.markdown("### Overview of top 1000 Addresses (The results are for the default time period.)")
    st.dataframe(styled_table, use_container_width=True)
else:
    st.warning("No data available for top delegators in the selected period.")

# --- Row10: Charts ---------------------------------------------------------------------------------------------------
if not users_breakdown_df.empty:
    col1, col2 = st.columns(2)

    # --- Chart 1: Share of Users (Donut) ----------------------------------------------------------------------------
    with col1:
        fig1 = go.Figure(data=[
            go.Pie(
                labels=users_breakdown_df["Category"],
                values=users_breakdown_df["Users Count"],
                hole=0.4,
                textinfo="label+percent",
                hovertemplate="%{label}: %{value} Users"
            )
        ])
        fig1.update_layout(
           title="Share of Users",
           height=400,
           legend=dict(
              x=1,       
              y=0.5,      
              xanchor="left",  
              yanchor="middle",
              orientation="v"  
           )
         )
        st.plotly_chart(fig1, use_container_width=True)

    # --- Chart 2: Breakdown of Users (Clustered Bar) ----------------------------------------------------------------
    with col2:
        fig2 = go.Figure()
        for t in users_breakdown_df["Type"].unique():
            df_type = users_breakdown_df[users_breakdown_df["Type"] == t]
            fig2.add_bar(
                x=df_type["Category"],
                y=df_type["Users Count"],
                name=t,
                text=df_type["Users Count"],
                textposition="outside"
            )
        fig2.update_layout(
            barmode="group",
            title="Breakdown of Users",
            xaxis_title="Category",
            yaxis_title="Users Count",
            height=400,
            legend=dict(x=1, y=0.5, xanchor="left", yanchor="middle", orientation="v")
        )
        st.plotly_chart(fig2, use_container_width=True)
else:
    st.warning("No data available for users breakdown in the selected period.")

# --- Row11: KPIs -------------------------------------------------------------------------------------------------------
if not new_delegators_df.empty:
    total_new = int(new_delegators_df["Total Number of New Delegators"].iloc[0])
    avg_daily = int(new_delegators_df["Avg Number of Daily Delegators"].iloc[0])

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            f"""
            <div style="text-align: center; padding: 30px; background-color: #f8f9fa; border-radius: 15px; margin: 10px 0;">
                <h2 style="font-size: 32px; margin-bottom: 10px;">Total Number of New Delegators</h2>
                <p style="font-size: 48px; font-weight: bold; color: #2e7d32;">{total_new:,}</p>
                <p style="font-size: 16px; color: #6c757d;">Since January 2025</p>
            </div>
            """,
            unsafe_allow_html=True
        )

    with col2:
        st.markdown(
            f"""
            <div style="text-align: center; padding: 30px; background-color: #f8f9fa; border-radius: 15px; margin: 10px 0;">
                <h2 style="font-size: 32px; margin-bottom: 10px;">Avg Number of Daily Delegators</h2>
                <p style="font-size: 48px; font-weight: bold; color: #1565c0;">{avg_daily:,}</p>
                <p style="font-size: 16px; color: #6c757d;">Since January 2025</p>
            </div>
            """,
            unsafe_allow_html=True
        )
else:
    st.warning("No data available for new delegators.")

# --- Row12: Monthly New Delegators -----------------------------------------------------------------------------------
if not monthly_new_delegators.empty:
    fig = go.Figure()

    # Bar chart for New Delegators
    fig.add_bar(
        x=monthly_new_delegators["Month"],
        y=monthly_new_delegators["New Delegators"],
        name="New Delegators",
        marker_color="steelblue",
        yaxis="y1"
    )

    # Line chart for Cumulative New Delegators
    fig.add_scatter(
        x=monthly_new_delegators["Month"],
        y=monthly_new_delegators["Cumulative New Delegators"],
        name="Cumulative New Delegators",
        mode="lines+markers",
        line=dict(color="orange", width=3),
        yaxis="y2"
    )

    fig.update_layout(
        title="Monthly New Delegators",
        xaxis=dict(title="Month"),
        yaxis=dict(title="User count", side="left"),
        yaxis2=dict(title="User count", overlaying="y", side="right"),
        height=500,
        legend=dict(x=0, y=1.1, orientation="h")
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No data available for Monthly New Delegators in the selected period.")

# --- Row12: Two Charts Side by Side ------------------------------------------------------------------------------------
col1, col2 = st.columns(2)

# Normalized Area Chart
with col1:
    if not daily_share.empty:
        fig1 = px.area(
            daily_share,
            x="Date",
            y="Delegated Amount",
            color="Type",
            groupnorm="fraction",  # normalized
            title="Daily Share of Delegated Amount (60D)",
            labels={"Delegated Amount": "%share"}
        )
        fig1.update_layout(
            yaxis=dict(tickformat=".0%"),
            height=450,
            legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center")
        )
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.warning("No data available for Daily Share of Delegated Amount (60D).")

# Donut Chart
with col2:
    if not share_amount.empty:
        fig2 = go.Figure(
            data=[
                go.Pie(
                    labels=share_amount["Type"],
                    values=share_amount["Delegated Amount"],
                    hole=0.4,
                    textinfo="label+percent",
                    hovertemplate="%{label}: %{value} AXL",
                )
            ]
        )
        fig2.update_layout(
            title="Share of Amount (60D)",
            height=450,
            legend=dict(orientation="v", x=1.1, y=0.5)
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.warning("No data available for Share of Amount (60D).")

# --- Row14: KPI for Active Validators ----------------------------------------------------------------------------------
st.markdown(
    """
    <div style="background-color:#fc0060; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Validators</h2>
    </div>
    """,
    unsafe_allow_html=True
)

active_validators_value = monthly_validators["Active Validators"].iloc[-1] if not monthly_validators.empty else None

if active_validators_value is not None:
    st.markdown(
        f"""
        <div style="text-align: center; padding: 1px; background-color: #f8f9fa; border-radius: 1px; margin: 10px 0;">
            <h2 style="font-size: 32px; margin-bottom: 10px;">Active Validators</h2>
            <p style="font-size: 48px; font-weight: bold; color: #1565c0;">{active_validators_value:,}</p>
            <p style="font-size: 16px; color: #6c757d;">Last Stat</p>
        </div>
        """,
        unsafe_allow_html=True
    )
else:
    st.warning("No data available for Active Validators in the selected period.")
# --- Row15: Chart for Monthly New Validators ---------------------------------------------------------------------------
if not monthly_validators.empty:
    fig = go.Figure()

    # Bar: New Validators
    fig.add_trace(go.Bar(
        x=monthly_validators["Month"],
        y=monthly_validators["New Validators"],
        name="New Validators",
        yaxis="y2",
        marker_color="#42a5f5"
    ))

    # Line: Cumulative New Validators
    fig.add_trace(go.Scatter(
        x=monthly_validators["Month"],
        y=monthly_validators["Cumulative New Validators"],
        name="Cumulative New Validators",
        mode="lines+markers",
        line=dict(color="#ef5350", width=2),
        yaxis="y1"
    ))

    fig.update_layout(
        title="Monthly New Validators",
        xaxis=dict(title="Month"),
        yaxis=dict(
            title="Validators count",
            side="left",
            showgrid=False
        ),
        yaxis2=dict(
            title="Validators count",
            side="right",
            overlaying="y"
        ),
        height=500,
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
        barmode="group"
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No data available for Monthly New Validators in the selected period.")

# -- Row 16 -----------------------------------------
col1, col2 = st.columns(2)

# --- Bar-Line Chart: Top 10 Validators Based on Redelegate Amount ---
with col1:
    if not redelegate_data.empty:
        fig_bar_line = go.Figure()

        # Bar: Redelegate Amount
        fig_bar_line.add_trace(go.Bar(
            x=redelegate_data["Validator"],
            y=redelegate_data["Redelegate Amount"],
            name="Redelegate Amount",
            yaxis="y1",
            marker_color="#42a5f5"
        ))

        # Line: Avg Amount
        fig_bar_line.add_trace(go.Scatter(
            x=redelegate_data["Validator"],
            y=redelegate_data["Avg Amount"],
            name="Avg Amount",
            yaxis="y2",
            mode="lines+markers",
            line=dict(color="#ff9800", width=2)
        ))

        fig_bar_line.update_layout(
            title="Top 10 Validators Based on Redelegate Amount",
            xaxis=dict(title="Validator"),
            yaxis=dict(
                title="$AXL",
                side="left",
                showgrid=False
            ),
            yaxis2=dict(
                title="$AXL",
                side="right",
                overlaying="y"
            ),
            height=500,
            # -- legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
            legend=dict(x=0, y=1.1, orientation="h"),
            barmode="group"
        )

        st.plotly_chart(fig_bar_line, use_container_width=True)
    else:
        st.warning("No data available for redelegate amounts.")

# --- Pie Chart: Share of Transactions ---
with col2:
    if not redelegate_data.empty:
        fig_pie = go.Figure(data=[
            go.Pie(
                labels=redelegate_data["Validator"],
                values=redelegate_data["Transactions"],
                textinfo="label+percent",
                hovertemplate="%{label}: %{value} Transactions",
                marker=dict(colors=px.colors.qualitative.Set3)
            )
        ])

        fig_pie.update_layout(
            title="Share of Transactions",
            height=500,
            legend=dict(x=1, y=0.5, orientation="v")  # labels on the right
        )

        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.warning("No data available for transactions.")

# --- Row17: Plot Horizontal Bar Chart ------------------------------------------------------------------------
if not net_delegate_data.empty:
    fig = go.Figure(go.Bar(
        x=net_delegate_data["Net Delegate Amount"],
        y=net_delegate_data["Validator"],
        orientation='h',
        marker=dict(
            color=net_delegate_data["Net Delegate Amount"],
            colorscale='Blues'
        ),
        text=net_delegate_data["Net Delegate Amount"],
        textposition='outside'
    ))

    fig.update_layout(
        title="Current Net Delegated Per Validator",
        xaxis_title="Net Delegate Amount (AXL)",
        yaxis_title="Validator",
        height=1000,
        yaxis=dict(autorange="reversed")  
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No data available for Net Delegated Amount per Validator.")

# --- Reference and Rebuild Info ---------------------------------------------------------------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="margin-top: 20px; margin-bottom: 20px; font-size: 16px;">
        <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 10px;">
            <img src="https://cdn-icons-png.flaticon.com/512/3178/3178287.png" alt="Reference" style="width:20px; height:20px;">
            <span>Dashboard Reference: <a href="https://flipsidecrypto.xyz/hess/axl-staking-stats-X1tELe" target="_blank">https://flipsidecrypto.xyz/hess/axl-staking-stats-X1tELe/</a></span>
        </div>
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://pbs.twimg.com/profile_images/1856738793325268992/OouKI10c_400x400.jpg" alt="Flipside" style="width:25px; height:25px; border-radius: 50%;">
            <span>Data Powered by: <a href="https://flipsidecrypto.xyz/home/" target="_blank">Flipside</a></span>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Links with Logos ---------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="font-size: 16px;">
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://axelarscan.io/logos/logo.png" alt="Axelar" style="width:20px; height:20px;">
            <a href="https://www.axelar.network/" target="_blank">https://www.axelar.network/</a>
        </div>
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://cdn-icons-png.flaticon.com/512/5968/5968958.png" alt="X" style="width:20px; height:20px;">
            <a href="https://x.com/axelar" target="_blank">https://x.com/axelar</a>
        </div>
        
    </div>
    """,
    unsafe_allow_html=True
)

