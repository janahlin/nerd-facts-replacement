import os
import shutil
from swapi import ingest_swapi
from netrunner import ingest_netrunner
from pokeapi import ingest_pokeapi

# ✅ Define paths
DB_FILE = "dbt_project/data/nerd_facts.duckdb"
DEST_FOLDER = "dbt_project/reports/sources/nerd_facts"
DEST_FILE = os.path.join(DEST_FOLDER, "nerd_facts.duckdb")

if __name__ == "__main__":
    print("🚀 Starting data ingestion process...")

    # ✅ Ingest SWAPI Data
    ingest_swapi()

    # ✅ Ingest Netrunner Data
    ingest_netrunner()

    # ✅ Ingest Pokémon Data
    ingest_pokeapi(limit=50)  # Fetch first 50 Pokémon for demo

    print("✅ All data ingestion tasks completed!")

    # ✅ Conditionally Copy DuckDB File
    if os.path.exists(DEST_FOLDER):  # Only copy if folder exists
        try:
            shutil.copy2(DB_FILE, DEST_FILE)  # Overwrite existing file
            print(f"✅ Copied {DB_FILE} → {DEST_FILE}")
        except Exception as e:
            print(f"❌ Error copying file: {e}")
    else:
        print(f"⚠️ Skipping copy: Folder {DEST_FOLDER} does not exist.")