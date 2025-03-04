import os
import duckdb
import requests
import pandas as pd
import re
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# Construct DB_PATH dynamically
BASE_DIR = os.getcwd()
DB_PATH = os.path.join(BASE_DIR, 'dbt_project', 'data', 'nerd_facts.duckdb')
SCHEMA = 'raw'

# ✅ Ensure the DuckDB file directory exists
def ensure_db_directory(db_path):
    directory = os.path.dirname(db_path)
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")

# ✅ Extract UIDs from URL list
def extract_uids(url_list, entity):
    if not isinstance(url_list, list):
        return ""
    pattern = rf"https://www\.swapi\.tech/api/{entity}/(\d+)"
    return ",".join(re.findall(pattern, " ".join(url_list)))

# ✅ Fetch and flatten film data
def flatten_film(record):
    properties = record.get("properties", {})
    return {
        "uid": str(record.get("uid")),  # ✅ Ensure UID is stored as a string
        "title": properties.get("title"),
        "episode_id": properties.get("episode_id"),
        "director": properties.get("director"),
        "producer": properties.get("producer"),
        "release_date": properties.get("release_date"),
        "opening_crawl": properties.get("opening_crawl"),
        "characters": extract_uids(properties.get("characters", []), "people"),
        "planets": extract_uids(properties.get("planets", []), "planets"),
        "starships": extract_uids(properties.get("starships", []), "starships"),
        "vehicles": extract_uids(properties.get("vehicles", []), "vehicles"),
        "species": extract_uids(properties.get("species", []), "species"),
        "created": properties.get("created"),
        "edited": properties.get("edited"),
        "url": properties.get("url")
    }

# ✅ Fix flattening for all other entities (people, planets, etc.)
def flatten_entity(record):
    result = record.get("result", {})
    properties = result.get("properties", {})

    # ✅ Extract `uid` from `result`
    uid = result.get("uid")
    if uid is None:
        print(f"⚠️ Warning: UID missing in {json.dumps(record, indent=2)}")  # Debugging print

    return {"uid": str(uid)} | properties  # ✅ Ensure UID is first and not NULL

# ✅ Fetch all films (Now Defined!)
def fetch_films():
    url = "https://www.swapi.tech/api/films"
    print(f"Fetching films from {url}...")

    with requests.Session() as session:
        data = fetch_data(url, session)

    if not data or "result" not in data:
        print("⚠️ Warning: No films data returned from API!")
        return []

    films = [flatten_film(rec) for rec in data["result"]]
    print(f"✅ Successfully fetched {len(films)} films.")
    return films

# ✅ Updated Fetch Data Function (Handles Timeouts & Retries)
def fetch_data(url, session=None, retries=5, timeout=30):
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=timeout) if session else requests.get(url, timeout=timeout)
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

# ✅ Fetch all pages for an entity (Handles Timeouts)
def fetch_all_entity_pages(entity, session):
    base_url = f"https://www.swapi.tech/api/{entity}"
    all_results = []
    page = 1

    while True:
        url = f"{base_url}?page={page}&limit=10"
        data = fetch_data(url, session, timeout=30)  # Increased timeout
        if not data or "results" not in data:
            break

        all_results.extend(data["results"])
        if not data.get("next"):
            break
        page += 1

    return all_results

# ✅ Fetch full details for each entity UID (Handles Timeouts)
def fetch_full_entity_data(entity):
    with requests.Session() as session:
        overview = fetch_all_entity_pages(entity, session)
        if not overview:
            return []

        def fetch_detail(record):
            uid = record.get("uid")
            if not uid:
                return None
            detail_url = f"https://www.swapi.tech/api/{entity}/{uid}"
            detail_data = fetch_data(detail_url, session, timeout=30)  # Increased timeout
            return flatten_entity(detail_data) if detail_data else None

        detailed_data = []
        with ThreadPoolExecutor(max_workers=3) as executor:  # Reduced parallel workers
            future_to_record = {executor.submit(fetch_detail, rec): rec for rec in overview}
            for future in as_completed(future_to_record):
                try:
                    result = future.result()
                    if result:
                        detailed_data.append(result)
                except Exception as e:
                    print(f"Error processing record: {e}")

    return detailed_data

# ✅ Store data in DuckDB (Ensures `raw` schema is used)
def load_to_duckdb(df, table_name, schema=SCHEMA, db_path=DB_PATH):
    ensure_db_directory(db_path)

    if df.empty:
        print(f"⚠️ Warning: No data for {table_name}, skipping.")
        return

    con = duckdb.connect(db_path)

    # ✅ Ensure the `uid` column exists and is first
    if "uid" not in df.columns:
        print(f"⚠️ Warning: 'uid' missing from {table_name}, adding dummy values.")
        df["uid"] = None  # Add a placeholder UID column if missing

    df = df[["uid"] + [col for col in df.columns if col != "uid"]]  # Move `uid` to the first column

    # ✅ Use DuckDB's native SQL
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
    con.execute(f"CREATE TABLE {schema}.{table_name} AS SELECT * FROM df")

    con.close()
    print(f"✅ Loaded {len(df)} records into {schema}.{table_name}.")

# ✅ Main execution
if __name__ == "__main__":
    print(f"Using DuckDB database at: {DB_PATH}")

    # ✅ Fetch & Store Films
    print("Fetching Films...")
    films_data = fetch_films()
    df_films = pd.DataFrame(films_data)
    load_to_duckdb(df_films, "films")

    # ✅ Fetch & Store Other Entities
    entities = ["people", "planets", "starships", "species", "vehicles"]

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_full_entity_data, entity): entity for entity in entities}

        for future in as_completed(futures):
            entity = futures[future]
            try:
                print(f"Fetching {entity.capitalize()}...")
                entity_data = future.result()
                df_entity = pd.DataFrame(entity_data)
                load_to_duckdb(df_entity, entity)
            except Exception as e:
                print(f"Error fetching {entity}: {e}")

    print("✅ Data successfully loaded into DuckDB.")
