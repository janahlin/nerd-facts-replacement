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

# Fetch Data with Retries
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

# Fetch All Resources with Pagination
def fetch_all_resources(endpoint, limit):
    url = f"https://pokeapi.co/api/v2/{endpoint}/?limit={limit}"
    resources = []
    while url:
        data = fetch_data(url)
        if not data or "results" not in data:
            print(f"⚠️ Warning: No data from {url}!")
            break
        resources.extend(data["results"])
        url = data.get("next")
    return resources

# Fetch Detailed Data for a Resource
def fetch_resource_detail(url):
    return fetch_data(url)

# Flatten JSON Structure
def flatten_json(nested_json, parent_key='', sep='_'):
    flattened = {}
    for key, value in nested_json.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            flattened.update(flatten_json(value, new_key, sep))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    flattened.update(flatten_json(item, f"{new_key}_{i}", sep))
                else:
                    flattened[f"{new_key}_{i}"] = item
        else:
            flattened[new_key] = value
    return flattened

# Load Data into DuckDB
def load_to_duckdb(df, table_name, schema=SCHEMA, db_path=DB_PATH):
    ensure_db_directory(db_path)
    if df.empty:
        print(f"⚠️ No data for {table_name}, skipping.")
        return
    con = duckdb.connect(db_path)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
    con.execute(f"CREATE TABLE {schema}.{table_name} AS SELECT * FROM df")
    con.close()
    print(f"✅ Loaded {len(df)} records into {schema}.{table_name}.")

# Recursive Processing of Resources
def process_resource(resource, resource_type, processed, all_data):
    if "url" not in resource:
        return
    resource_url = resource["url"]
    if resource_url in processed:
        return
    processed.add(resource_url)

    detailed_data = fetch_resource_detail(resource_url)
    if not detailed_data:
        return

    flat_data = flatten_json(detailed_data)
    all_data[resource_type].append(flat_data)

    # Recursively process nested URLs
    for key, value in detailed_data.items():
        if isinstance(value, dict) and "url" in value:
            process_resource(value, key, processed, all_data)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict) and "url" in item:
                    process_resource(item, key, processed, all_data)

# Main Function to Ingest Data
def ingest_pokeapi(limit=50):
    print("🚀 Fetching Data from PokéAPI...")
    all_data = {endpoint.replace('-', '_'): [] for endpoint in endpoints}
    processed_urls = set()

    for endpoint in endpoints:
        print(f"Fetching {endpoint}...")
        resources = fetch_all_resources(endpoint, limit=50)
        if not resources:
            print(f"⚠️ No data for {endpoint}, skipping.")
            continue

        # Fetch detailed data (Parallel Processing)
        print(f"Fetching {len(resources)} entries for {endpoint}...")
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {executor.submit(fetch_resource_detail, res['url']): res['url'] for res in resources}
            for future in as_completed(future_to_url):
                try:
                    result = future.result()
                    if result:
                        process_resource(result, endpoint.replace('-', '_'), processed_urls, all_data)
                except Exception as e:
                    print(f"❌ Error processing {future_to_url[future]}: {e}")

    # Store in DuckDB
    for entity, data in all_data.items():
        if data:
            df = pd.DataFrame(data)
            load_to_duckdb(df, f"{TABLE_PREFIX}{entity}")

    print("✅ PokéAPI ingestion complete.")

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
if __name__ == "__main__":
    ingest_pokeapi(limit=50)  # Fetch first 50 Pokémon for demo