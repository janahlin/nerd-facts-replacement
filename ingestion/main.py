import duckdb
import requests

DB_FILE = "nerd_facts.duckdb"

def fetch_data():
    url = "https://swapi.dev/api/people/"
    response = requests.get(url)
    data = response.json()["results"]
    return [(person["name"], person["height"], person["mass"], person["hair_color"]) for person in data]

def store_data():
    db = duckdb.connect(DB_FILE)
    db.execute("CREATE TABLE IF NOT EXISTS swapi_people (name TEXT, height INTEGER, mass INTEGER, hair_color TEXT)")
    db.executemany("INSERT INTO swapi_people VALUES (?, ?, ?, ?)", fetch_data())
    db.close()

if __name__ == "__main__":
    store_data()
    print("✅ Data Ingestion Complete!")
