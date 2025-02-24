import requests
import pandas as pd
import json
import os

DATA_DIR = "dbt_project/data/raw"

# Fetch data for main entities
entities = ['people', 'planets', 'films', 'species', 'vehicles', 'starships']

def fetch_data(entity):
    results = []
    url = f"https://swapi.dev/api/{entity}/"
    
    try:
        while url:
            response = requests.get(url)
            response.raise_for_status()  # Raise exception for bad status codes
            data = response.json()
            results.extend(data["results"])
            url = data["next"]  # Get next page if available
        return results
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {entity}: {e}")
        return []

def main():
    # Ensure the raw data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)
    
    for entity in entities:
        print(f"Fetching {entity}...")
        data = fetch_data(entity)
        
        if not data:  # Skip if no data was fetched
            continue
        
        try:            
            # Convert to DataFrame and handle nested data
            df = pd.DataFrame(data)
            
            # Handle nested data
            for col in df.columns:
                if isinstance(df[col].iloc[0], list):
                    df[col] = df[col].apply(lambda x: ','.join(x) if x else '')
                
                # Remove commas from numeric columns
                if df[col].dtype == object:
                    df[col] = df[col].str.replace(',', '', regex=True)
            
            # Save as CSV
            csv_filepath = os.path.join(DATA_DIR, f"{entity}.csv")
            df.to_csv(csv_filepath, index=False)
            print(f"✅ CSV saved: {csv_filepath}")
                        
            
        except Exception as e:
            print(f"Error processing {entity}: {e}")
            continue
    
    print("✅ All SWAPI data fetched and stored!")

if __name__ == "__main__":
    main()