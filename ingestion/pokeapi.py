import os
import duckdb
import requests
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Database Config
BASE_DIR = os.getcwd()
DB_PATH = os.path.join(BASE_DIR, 'dbt_project/data/nerd_facts.duckdb')
SCHEMA = 'raw'
TABLE_PREFIX = 'pokemon_'

# Ensure DuckDB Directory Exists
def ensure_db_directory(db_path):
    directory = os.path.dirname(db_path)
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")

# Fetch Data from PokéAPI with Retries
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

# Fetch All Resources from an Endpoint with Pagination
def fetch_all_resources(endpoint):
    url = f"https://pokeapi.co/api/v2/{endpoint}/?limit=100"
    resources = []
    while url:
        data = fetch_data(url)
        if not data or "results" not in data:
            print(f"⚠️ Warning: No data returned from {url}!")
            break
        resources.extend(data["results"])
        url = data.get("next")
    return resources

# Fetch Detailed Data for a Resource
def fetch_resource_detail(url):
    return fetch_data(url)

# Load Data into DuckDB
def load_to_duckdb(df, table_name, schema=SCHEMA, db_path=DB_PATH):
    ensure_db_directory(db_path)
    if df.empty:
        print(f"⚠️ Warning: No data for {table_name}, skipping.")
        return
    con = duckdb.connect(db_path)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
    con.execute(f"CREATE TABLE {schema}.{table_name} AS SELECT * FROM df")
    con.close()
    print(f"✅ Loaded {len(df)} records into {schema}.{table_name}.")

# Main Function to Ingest Data from Specified Endpoints
def ingest_pokeapi(endpoints):
    print("🚀 Fetching Data from PokéAPI...")

    for endpoint in endpoints:
        print(f"Fetching data for endpoint: {endpoint}...")
        resources = fetch_all_resources(endpoint)
        if not resources:
            print(f"⚠️ No resources found for {endpoint}, skipping.")
            continue

        # Fetch detailed data for each resource (Parallel Processing)
        print(f"Fetching detailed data for {len(resources)} {endpoint} entries...")
        detailed_data = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {executor.submit(fetch_resource_detail, res['url']): res['url'] for res in resources}
            for future in as_completed(future_to_url):
                try:
                    result = future.result()
                    if result:
                        detailed_data.append(result)
                except Exception as e:
                    print(f"❌ Error processing URL {future_to_url[future]}: {e}")

        # Flatten nested structures if necessary
        if detailed_data:
            df = pd.json_normalize(detailed_data)
            table_name = f"{TABLE_PREFIX}{endpoint.replace('-', '_')}"
            load_to_duckdb(df, table_name)
        else:
            print(f"⚠️ No detailed data retrieved for {endpoint}.")

    print("✅ Data ingestion from PokéAPI completed.")

# List of endpoints to ingest
endpoints = [
    "berry", "berry-firmness", "berry-flavor", "contest-type", "contest-effect",
    "super-contest-effect", "encounter-method", "encounter-condition", "encounter-condition-value",
    "evolution-chain", "evolution-trigger", "generation", "pokedex", "version", "version-group",
    "item", "item-attribute", "item-category", "item-fling-effect", "item-pocket", "machine",
    "move", "move-ailment", "move-battle-style", "move-category", "move-damage-class",
    "move-learn-method", "move-target", "location", "location-area", "pal-park-area", "region",
    "ability", "characteristic", "egg-group", "gender", "growth-rate", "nature", "pokeathlon-stat",
    "pokemon", "pokemon-color", "pokemon-form", "pokemon-habitat", "pokemon-shape", "pokemon-species",
    "stat", "type", "language"
]

# Run the ingestion process
ingest_pokeapi(endpoints)
