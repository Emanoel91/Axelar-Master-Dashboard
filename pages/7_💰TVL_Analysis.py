import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
import streamlit as st
import requests

# --- Page Config: Tab Title & Icon ---
st.set_page_config(
    page_title="Axelar Master Dashboard",
    page_icon="https://axelarscan.io/logos/logo.png",
    layout="wide"
)

# reading data-------------------------------------------------------
df = pd.read_csv("tvl_data.csv")

# convert date to supported date format -------------------------------------------
df["date"] = pd.to_datetime(df["date"], format="mixed", errors="coerce")
df = df.dropna(subset=["date"])

# Data type assurance ---------------------------------------
df["tvl"] = pd.to_numeric(df["tvl"], errors="coerce")
df = df.dropna(subset=["tvl"])

# Data sorting
df = df.sort_values("date")

# --- Title ---------------------------------------------------------------------------------------------------
st.title("💰TVL Analysis")

# --- Row 1: Stacked Bar Chart ------------------

st.info("🔔To view the most recent updates, click on the '...' in the top-right corner of the page and select 'Rerun'.")

# Plot order for columns (ITS is placed above non-ITS) --------------------------------
category_order = {"asset_type": ["non-ITS", "ITS"]}

fig1 = px.bar(
    df,
    x="date",
    y="tvl",
    color="asset_type",
    title="Axelar TVL Over Time",
    labels={"tvl": "TVL ($USD)", "date": "Date"},
    category_orders=category_order,
)

# Calculate total daily TVL ---------------------
daily_total = df.groupby("date")["tvl"].sum().reset_index()

# Add TVL_total line on y-axis -----------------------------
fig1.add_trace(
    go.Scatter(
        x=daily_total["date"],
        y=daily_total["tvl"],
        mode="lines",
        name="Total TVL",
        line=dict(color="black", width=2),
        yaxis="y"
    )
)

st.plotly_chart(fig1, use_container_width=True)

# --- Row 2+ ----------------------------------------------------------------------------------------------------------------------------------------------------------

# --- Load API Data ---
@st.cache_data(ttl=3600)
def load_axelar_api():
    url = "https://api.axelarscan.io/api/getTVL"  
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Failed to fetch API data: {response.status_code}")
        return None

data = load_axelar_api()

# --- Parse Data ---
if data and "data" in data:
    rows = []
    for asset in data["data"]:
        asset_id = asset.get("asset", "")
        price = asset.get("price", None)
        total = asset.get("total", None)
        value = asset.get("value", None)
        asset_type = asset.get("assetType", "")
        abnormal = asset.get("is_abnormal_supply", False)

        tvl_data = asset.get("tvl", {})
        for chain, details in tvl_data.items():
            total_tvl = details.get("total", None)
            rows.append({
                "Asset ID": asset_id,
                "Asset Type": asset_type,
                "Chain": chain,
                "Token Symbol": details.get("contract_data", {}).get("symbol") if "contract_data" in details else None,
                "Token Name": details.get("contract_data", {}).get("name") if "contract_data" in details else None,
                "Contract Address": details.get("contract_data", {}).get("contract_address") if "contract_data" in details else None,
                "Gateway Address": details.get("gateway_address", None),
                "Supply": details.get("supply", None),
                "Total TVL": total_tvl,
                "Price (USD)": price,
                "TVL (USD)": round(total_tvl * price, 0) if total_tvl is not None and price is not None else None,
                "Total Asset Value (USD)": value,
                "Is Abnormal?": abnormal
            })

    df = pd.DataFrame(rows)

    # --- Format Numbers ---
    numeric_cols = ["Supply", "Total TVL", "Price (USD)", "TVL (USD)", "Total Asset Value (USD)"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- Display Table ---
# --    st.dataframe(df.style.format({
# --        "Supply": "{:,.2f}",
# --        "Total TVL": "{:,.2f}",
# --        "Price (USD)": "{:,.4f}",
# --        "TVL (USD)": "{:,.0f}",
# --        "Total Asset Value (USD)": "{:,.2f}"
# --    }), use_container_width=True)
else:
    st.warning("No data available from API.")
# ----------------------------------------------------------------------------------------------------------------------------------------------
unique_assets = df.drop_duplicates(subset=["Asset ID"])
total_axelar_tvl = unique_assets["Total Asset Value (USD)"].sum()

# --- KPI ---
st.markdown(
    f"""
    <div style="background-color:#d9f0fc; padding:20px; border-radius:15px; text-align:center;">
        <h2 style="color:#fc9608; font-size:22px; margin-bottom:5px;">Total Axelar TVL (Latest update)</h2>
        <h1 style="color:#006ac9; font-size:48px; font-weight:bold;">${total_axelar_tvl:,.0f}</h1>
    </div>
    """,
    unsafe_allow_html=True
)
# -----------
asset_type_df = unique_assets.copy()
asset_type_df["Asset Type Label"] = asset_type_df["Asset Type"].apply(
    lambda x: "ITS" if str(x).lower() == "its" else "non-ITS"
)
asset_type_summary = asset_type_df.groupby("Asset Type Label", as_index=False)["Total Asset Value (USD)"].sum()

fig_asset_type = px.pie(
    asset_type_summary,
    values="Total Asset Value (USD)",
    names="Asset Type Label",
    hole=0.5,
    color="Asset Type Label",
    color_discrete_map={"ITS": "#62cbff", "non-ITS": "#006ac9"},
    title="Share of TVL by Asset Type"
)
fig_asset_type.update_traces(textposition="inside", textinfo="percent+label")
fig_asset_type.update_layout(showlegend=True)

df["TVL (USD)"] = (df["Total TVL"] * df["Price (USD)"]).round(0)

chain_summary = df.groupby("Chain", as_index=False)["TVL (USD)"].sum()

fig_chain = px.pie(
    chain_summary,
    values="TVL (USD)",
    names="Chain",
    hole=0.5,
    title="Share of TVL by Chain"
)
fig_chain.update_traces(textposition="inside", textinfo="percent+label")
fig_chain.update_layout(showlegend=True)

col1, col2 = st.columns(2)
with col1:
    st.plotly_chart(fig_asset_type, use_container_width=True)
with col2:
    st.plotly_chart(fig_chain, use_container_width=True)
# ------------------------------------------------------------------------------------------------------------------------------------------------
# --- Load Chains API ---
@st.cache_data(ttl=3600)
def load_chains_api():
    url = "https://api.llama.fi/v2/chains"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Failed to fetch Chains API: {response.status_code}")
        return []

chains_data = load_chains_api()

chains_df = pd.DataFrame(chains_data)

chains_df = chains_df[["name", "tvl", "tokenSymbol"]]
chains_df.columns = ["Chain Name", "TVL (USD)", "Native Token Symbol"]

chains_df = pd.concat([
    chains_df,
    pd.DataFrame([{
        "Chain Name": "Axelar",
        "TVL (USD)": total_axelar_tvl,
        "Native Token Symbol": "AXL"
    }])
], ignore_index=True)

chains_df = chains_df.sort_values("TVL (USD)", ascending=False).reset_index(drop=True)

chains_df.index = chains_df.index + 1

st.markdown("### TVL of Different Chains")
st.dataframe(
    chains_df.style.format({
        "TVL (USD)": "{:,.0f}"
    }),
    use_container_width=True
)

# ----------------------------------------------------------------------------------------------------------------------------
top_20_chains = chains_df.head(20).reset_index()

def human_format(num):
    if num >= 1e9:
        return f"{num/1e9:.1f}B"
    elif num >= 1e6:
        return f"{num/1e6:.1f}M"
    elif num >= 1e3:
        return f"{num/1e3:.1f}K"
    else:
        return str(int(num))

# --- Bar Chart ---
fig_bar = px.bar(
    top_20_chains,
    x="Chain Name",
    y="TVL (USD)",
    color="Chain Name",
    text=top_20_chains["TVL (USD)"].apply(human_format),
    title="Top 20 Chains by TVL ($USD)"
)

fig_bar.update_traces(textposition="outside")
fig_bar.update_layout(
    xaxis_title="Chain",
    yaxis_title="$USD",
    showlegend=False,
    plot_bgcolor="white"
)

st.plotly_chart(fig_bar, use_container_width=True)
