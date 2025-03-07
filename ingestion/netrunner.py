import os
import duckdb
import requests
import pandas as pd
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ✅ Define database path
BASE_DIR = os.getcwd()
DB_PATH = os.path.join(BASE_DIR, 'dbt_project', 'data', 'nerd_facts.duckdb')
SCHEMA = 'raw'
BASE_URL = "https://netrunnerdb.com/api/2.0/public"

# ✅ Ensure DuckDB directory exists
def ensure_db_directory(db_path):
    directory = os.path.dirname(db_path)
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        print(f"📂 Created directory: {directory}")

# ✅ Function to fetch data with retries
def fetch_data(url, retries=5, timeout=30):
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            wait_time = (2 ** attempt) + (0.5 * attempt)  # Exponential backoff
            print(f"⚠️ Timeout: {url} - Retrying in {wait_time:.2f} sec...")
            time.sleep(wait_time)
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching {url}: {e}")
            break
    return None  # Return None if all retries fail

# ✅ Fetch all items from an endpoint
def fetch_all(endpoint):
    url = f"{BASE_URL}/{endpoint}"
    print(f"📥 Fetching data from {url}...")
    data = fetch_data(url)
    
    if not data or "data" not in data:
        print(f"⚠️ No data returned from {url}")
        return []
    
    records = data["data"]
    print(f"🔍 DEBUG: Sample data for {endpoint} → {json.dumps(records[:3], indent=2)}")  # Show sample records
    return records

# ✅ Fetch detailed info for each item
def fetch_details(endpoint, key_field):
    records = fetch_all(endpoint)
    if not records:
        return []

    details = []

    def fetch_detail(item):
        item_key = item.get(key_field)
        if not item_key:
            return None
        detail_url = f"{BASE_URL}/{endpoint[:-1]}/{item_key}"
        detail_data = fetch_data(detail_url)
        if detail_data and "data" in detail_data:
            return detail_data["data"][0]  # Extract the actual record

    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_item = {executor.submit(fetch_detail, rec): rec for rec in records}
        for future in as_completed(future_to_item):
            result = future.result()
            if result:
                details.append(result)

    print(f"🔍 DEBUG: Fetched {len(details)} detailed records for {endpoint}")
    return details

# ✅ Fetch and flatten all card details
def fetch_card_details():
    cards = fetch_all("cards")
    if not cards:
        return []

    details = []

    def fetch_card_detail(card):
        card_code = card.get("code")
        if not card_code:
            return None
        detail_url = f"{BASE_URL}/card/{card_code}"
        detail_data = fetch_data(detail_url)
        if detail_data and "data" in detail_data:
            return detail_data["data"][0]  # Extract detailed card info

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_card = {executor.submit(fetch_card_detail, card): card for card in cards}
        for future in as_completed(future_to_card):
            result = future.result()
            if result:
                details.append(result)

    print(f"🔍 DEBUG: Fetched {len(details)} detailed card records")
    return details

# ✅ Flatten and load data into DuckDB
def load_to_duckdb(data, table_name, schema=SCHEMA, db_path=DB_PATH):
    ensure_db_directory(db_path)

    if not data:
        print(f"⚠️ Warning: No data for {schema}.{table_name}, skipping insertion.")
        return    

    df = pd.json_normalize(data)  # ✅ Flatten nested JSON

    print(f"🔍 DEBUG: Data received for {schema}.{table_name} → {len(df)} records")
    print(df.head())  # Show first few rows

    con = duckdb.connect(db_path)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
    con.execute(f"CREATE TABLE {schema}.{table_name} AS SELECT * FROM df")

    count = con.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}").fetchone()[0]
    print(f"✅ Loaded {count} records into {schema}.{table_name}.")

    con.close()

# ✅ Ingest all NetrunnerDB data
def ingest_netrunner():
    print("🚀 Starting NetrunnerDB data ingestion...")

    # ✅ Fetch and insert detailed data
    detailed_endpoints = {
        "cycles": "code",
        "factions": "code",
        "packs": "code",
        "sides": "code",
        "types": "code",
    }
    
    for endpoint, key_field in detailed_endpoints.items():
        data = fetch_details(endpoint, key_field)
        load_to_duckdb(data, f"netrunner_{endpoint}")

    # ✅ Fetch and insert detailed card data
    card_data = fetch_card_details()
    load_to_duckdb(card_data, "netrunner_cards")

    # ✅ Fetch and insert simple datasets
    simple_endpoints = ["prebuilts", "reviews", "rulings"]
    
    for endpoint in simple_endpoints:
        data = fetch_all(endpoint)
        load_to_duckdb(data, f"netrunner_{endpoint}")

    print("✅ NetrunnerDB data ingestion completed!")

# ✅ Run script if executed directly
if __name__ == "__main__":
    ingest_netrunner()
