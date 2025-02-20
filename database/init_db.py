import duckdb

DB_FILE = "nerd_facts.duckdb"

def initialize_db():
    db = duckdb.connect(DB_FILE)
    db.execute("CREATE TABLE IF NOT EXISTS swapi_people (name TEXT, height INTEGER, mass INTEGER, hair_color TEXT)")
    db.close()
    print("✅ Database Initialized!")

if __name__ == "__main__":
    initialize_db()
