# Nerd Facts Replacement

## Overview
This repository ingests data from various APIs (SWAPI, Netrunner, Pokémon) and transforms it using dbt. It demonstrates how to combine Python-based data ingestion with dbt transformations to load data into a DuckDB database.

## Repository Structure
- **ingestion/**: Contains Python scripts for data ingestion.
  - `main.py`: Entry point that triggers data ingestion from SWAPI, Netrunner, and Pokémon.
  - `swapi.py`, `netrunner.py`, `pokeapi.py`: Modules for ingesting data from respective APIs.
- **dbt_project/**: Contains the dbt project for data transformations.
  - **models/**
    - **staging/**: Models that load raw CSV files (via DuckDB's CSV reader) from `data/raw`.
    - **dimensions/**: Transformation models that structure the data and load it into the `swapi` schema.
  - **seeds/**: (Optional) Place to store CSV seeds if you prefer to use dbt seed functionality.
  - `dbt_project.yml`: dbt project configuration file.
- **data/raw/**: Contains raw CSV files used as the source data for dbt transformations.

## Setup and Installation

### Python Environment
1. Create and activate a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate


2. Install required packages:
   ```bash
   pip install -r requirements.txt

DuckDB Setup
Ensure that DuckDB is installed. You can install it via pip:

   ```bash
   pip install duckdb

Or follow the instructions for your platform.

dbt Setup
Install dbt for DuckDB:

   ```bash
   pip install dbt-duckdb

Running the Project
Data Ingestion
Run the main ingestion script to load API data:

   ```bash
   python ingestion/main.py

This will execute ingestion for SWAPI, Netrunner, and Pokémon APIs.

dbt Transformations
Seeding/Raw Data:
You can seed the CSV files (if using dbt seeds) or rely on the raw CSV files in the data/raw folder.

   ```bash
   dbt seed

Transformations:
Run the dbt models to transform and load the data. The dimension models target the swapi schema in your DuckDB database:
```bash
dbt run

Configuration

Verify your DuckDB connection in your dbt profile (typically located at ~/.dbt/profiles.yml).
Adjust project-level configurations in dbt_project.yml, if needed.
Ensure CSV files in data/raw reflect the expected structure for the staging models located in dbt_project/models/staging.

Troubleshooting

If errors occur during ingestion (e.g., KeyError), verify that API responses or CSV formats match the expected structure.
For dbt issues, review the specific model (staging or dimension) that is failing, and check that all configuration settings (such as schema names) are correct.

Contributing
Contributions are welcome! Please open issues or pull requests with improvements or bug fixes.

License
[Include your license information here] ```