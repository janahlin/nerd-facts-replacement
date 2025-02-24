// evidence/src/routes/api/films/+server.js
import duckdb from 'duckdb';

export async function GET() {
  // Ange sökvägen till DuckDB-filen
  // Eftersom 'evidence' och 'dbt_project' är syskonmappar i repository-roten
  // är den relativa sökvägen från evidence: "./../dbt_project/data/nerd_facts.duckdb"
  const dbPath = './../dbt_project/data/nerd_facts.duckdb';

  // Skapa en ny DuckDB-databasanslutning
  const db = new duckdb.Database(dbPath);

  // Exekvera en fråga mot t.ex. tabellen 'dim_films' (som skapats av dbt)
  return new Promise((resolve, reject) => {
    db.all("SELECT * FROM dim_films", (err, rows) => {
      if (err) {
        reject(
          new Response(JSON.stringify({ error: err.message }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
          })
        );
      } else {
        resolve(
          new Response(JSON.stringify(rows), {
            headers: { 'Content-Type': 'application/json' }
          })
        );
      }
    });
  });
}
