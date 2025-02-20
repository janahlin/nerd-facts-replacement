import pandas as pd
import requests
from datetime import datetime

PARQUET_FILE = "staging_data/swapi_people.parquet"

def fetch_data():
    url = "https://swapi.dev/api/people/"
    response = requests.get(url)
    data = response.json()["results"]
    return [(person["name"], person["height"], person["mass"], person["hair_color"], datetime.utcnow()) for person in data]

def store_data():
    df = pd.DataFrame(fetch_data(), columns=["name", "height", "mass", "hair_color", "created_at"])
    df.to_parquet(PARQUET_FILE, index=False)

if __name__ == "__main__":
    store_data()
    print("✅ Data Ingestion Complete!")
