from swapi import ingest_swapi
from netrunner import ingest_netrunner
from pokeapi import ingest_pokeapi

if __name__ == "__main__":
    print("🚀 Starting data ingestion process...")

    # ✅ Ingest SWAPI Data
    ingest_swapi()

    # ✅ Ingest Netrunner Data
    ingest_netrunner()

    # ✅ Ingest Pokémon Data
    ingest_pokeapi(limit=50)  # Fetch first 50 Pokémon for demo

    print("✅ All data ingestion tasks completed!")
