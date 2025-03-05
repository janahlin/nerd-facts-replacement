import os
import duckdb
import requests
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Database Config
BASE_DIR = os.getcwd()
DB_PATH = os.path.join(BASE_DIR, '/dbt_project/data/nerd_facts.duckdb')
SCHEMA = 'raw'

# Ensure DuckDB Directory Exists
def ensure_db_directory(db_path):
    directory = os.path.dirname(db_path)
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")

# Fetch Data from NetrunnerDB API with Retries
def fetch_data(url, retries=5, timeout=30):
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            wait_time = (2 ** attempt) + (0.5 * attempt)
            print(f"⚠️ Timeout: {url} - Retrying in {wait_time:.2f} sec...")
            time.sleep(wait_time)
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching {url}: {e}")
            break
    return None

# Fetch and Flatten Data from Endpoint
def fetch_and_flatten(endpoint, entity_name):
    url = f"https://netrunnerdb.com/api/2.0/public/{endpoint}"
    print(f"Fetching {entity_name} from {url}...")

    data = fetch_data(url)
    if not data or "data" not in data:
        print(f"⚠️ Warning: No {entity_name} data returned from API!")
        return []

    print(f"✅ Successfully fetched {len(data['data'])} {entity_name}. Sample: {data['data'][:2]}")  # <-- Added sample print
    return data["data"]

# Load Data into DuckDB
def load_to_duckdb(df, table_name, schema=SCHEMA, db_path=DB_PATH):
    ensure_db_directory(db_path)

    if df.empty:
        print(f"⚠️ Warning: No data for {table_name}, skipping.")
        return

    print(f"✅ Inserting {len(df)} records into {schema}.{table_name}. Sample data:\n{df.head()}")  # <-- Added print

    con = duckdb.connect(db_path)

    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
    con.execute(f"CREATE TABLE {schema}.{table_name} AS SELECT * FROM df")

    con.close()
    print(f"✅ Loaded {len(df)} records into {schema}.{table_name}.")

# Main Function to Ingest Netrunner Data
def ingest_netrunner():
    print("🚀 Fetching NetrunnerDB Data...")

    endpoints = {
        "cards": "cards",
        "cycles": "cycles",
        "packs": "packs",
        "sides": "sides",
        "factions": "factions",
        "types": "types",
        "subtypes": "subtypes"
    }

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_and_flatten, endpoint, name): name for name, endpoint in endpoints.items()}

        for future in as_completed(futures):
            entity_name = futures[future]
            try:
                data = future.result()
                if data:
                    df = pd.DataFrame(data)
                    load_to_duckdb(df, f"netrunner_{entity_name}")
            except Exception as e:
                print(f"❌ Error processing {entity_name}: {e}")

    print("✅ Netrunner Data successfully loaded into DuckDB.")

if __name__ == "__main__":
    ingest_netrunner()
