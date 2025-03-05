import os
import duckdb
import requests
import pandas as pd
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# ✅ Database Config
BASE_DIR = os.getcwd()
DB_PATH = os.path.join(BASE_DIR, '../data/nerd_facts.duckdb')
SCHEMA = 'raw'

# ✅ Ensure DuckDB Directory Exists
def ensure_db_directory(db_path):
    directory = os.path.dirname(db_path)
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")

# ✅ Fetch Data from PokeAPI with Retries
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

# ✅ Fetch All Pokémon IDs
def fetch_all_pokemon_ids(limit=100):
    url = f"https://pokeapi.co/api/v2/pokemon?limit={limit}"
    print(f"Fetching Pokémon list from {url}...")

    data = fetch_data(url)
    if not data or "results" not in data:
        print("⚠️ Warning: No Pokémon data returned from API!")
        return []

    return [entry["url"].split("/")[-2] for entry in data["results"]]

# ✅ Fetch Pokémon Details
def fetch_pokemon_detail(pokemon_id):
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_id}"
    data = fetch_data(url)

    if not data:
        return None

    return {
        "id": data["id"],
        "name": data["name"],
        "height": data["height"],
        "weight": data["weight"],
        "base_experience": data["base_experience"],
        "types": ", ".join([t["type"]["name"] for t in data["types"]]),
        "abilities": ", ".join([a["ability"]["name"] for a in data["abilities"]]),
        "stats": json.dumps({s["stat"]["name"]: s["base_stat"] for s in data["stats"]}),
        "sprite_url": data["sprites"]["front_default"]
    }

# ✅ Fetch Pokémon Species Data
def fetch_pokemon_species(pokemon_id):
    url = f"https://pokeapi.co/api/v2/pokemon-species/{pokemon_id}"
    data = fetch_data(url)

    if not data:
        return None

    return {
        "id": data["id"],
        "name": data["name"],
        "color": data["color"]["name"],
        "habitat": data["habitat"]["name"] if data["habitat"] else None,
        "shape": data["shape"]["name"] if data["shape"] else None,
        "base_happiness": data["base_happiness"],
        "capture_rate": data["capture_rate"]
    }

# ✅ Load Data into DuckDB
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

# ✅ Main Function to Ingest Pokémon Data
def ingest_pokeapi(limit=100):
    print("🚀 Fetching Pokémon Data...")

    # ✅ Step 1: Fetch Pokémon IDs
    pokemon_ids = fetch_all_pokemon_ids(limit=limit)
    if not pokemon_ids:
        print("⚠️ No Pokémon IDs retrieved, stopping.")
        return

    # ✅ Step 2: Fetch Pokémon Details (Parallel Processing)
    print("Fetching Pokémon Details...")
    pokemon_data = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_pokemon = {executor.submit(fetch_pokemon_detail, pid): pid for pid in pokemon_ids}
        for future in as_completed(future_to_pokemon):
            try:
                result = future.result()
                if result:
                    pokemon_data.append(result)
            except Exception as e:
                print(f"❌ Error processing Pokémon ID {future_to_pokemon[future]}: {e}")

    # ✅ Step 3: Store Pokémon Details in DuckDB
    if pokemon_data:
        df_pokemon = pd.DataFrame(pokemon_data)
        load_to_duckdb(df_pokemon, "pokemon")

    # ✅ Step 4: Fetch Pokémon Species (Parallel Processing)
    print("Fetching Pokémon Species...")
    species_data = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_species = {executor.submit(fetch_pokemon_species, pid): pid for pid in pokemon_ids}
        for future in as_completed(future_to_species):
            try:
                result = future.result()
                if result:
                    species_data.append(result)
            except Exception as e:
                print(f"❌ Error processing Pokémon Species ID {future_to_species[future]}: {e}")

    # ✅ Step 5: Store Pokémon Species in DuckDB
    if species_data:
        df_species = pd.DataFrame(species_data)
        load_to_duckdb(df_species, "pokemon_species")

    print("✅ Pokémon Data successfully loaded into DuckDB.")
