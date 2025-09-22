import pandas as pd
import requests
from datetime import date

CSV_FILE = "tvl_data.csv"
API_URL = "https://api.dune.com/api/v1/query/5535180/results?api_key=kmCBMTxWKBxn6CVgCXhwDvcFL1fBp6rO"

try:
    df = pd.read_csv(CSV_FILE)
    df["date"] = df["date"].str.slice(0, 10)
    df["date"] = pd.to_datetime(df["date"]).dt.date
except FileNotFoundError:
    df = pd.DataFrame(columns=["date", "tvl", "asset_type"])

last_date = df["date"].max() if not df.empty else None

if last_date != date.today():
    response = requests.get(API_URL)
    if response.status_code == 200:
        data = response.json()
        if "result" in data and "rows" in data["result"]:
            new_rows = pd.DataFrame(data["result"]["rows"])
            new_rows["date"] = pd.to_datetime(new_rows["date"]).dt.date

            today_rows = new_rows[new_rows["date"] == date.today()]
            if not today_rows.empty:
                df = pd.concat([df, today_rows], ignore_index=True)
                df.to_csv(CSV_FILE, index=False)
                print("New data has been received.")
            else:
                print("No data found for today.")
        else:
            print("Error: API Data structure has been changed.")
    else:
        print(f"API data receiving error: {response.status_code}")
else:
    print("Today data has been recorded previously.")
