import requests
import pandas as pd

urls = {
    "films": "https://swapi.dev/api/films/",
    "characters": "https://swapi.dev/api/people/",
    "pokemon": "https://pokeapi.co/api/v2/pokemon?limit=10",
}

def fetch_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return None

for name, url in urls.items():
    data = fetch_data(url)
    if data:
        df = pd.DataFrame(data["results"] if "results" in data else data)
        df.to_csv(f"seeds/{name}.csv", index=False)
        print(f"Saved {name}.csv")
