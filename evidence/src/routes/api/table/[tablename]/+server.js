// evidence/src/routes/api/table/[tablename]/+server.js
import duckdb from 'duckdb';

/**
 * GET /api/table/[tablename]
 * Exempel: /api/table/films
 */
export async function GET({ params }) {
  const schema = 'main';
  const { tablename } = params;
  
  // Lista med tillåtna tabeller (för säkerhet)
  const allowedTables = [
    'films', 
    'people', 
    'planets', 
    'species', 
    'starships', 
    'vehicles',
    'film_characters',
    'film_planets',
    'film_species',
    'film_starships',
    'film_vehicles',
    'people_planets',
    'people_species',
    'people_starships',
    'people_vehicles',
    'species_planets',
  ];  

  if (!allowedTables.includes(tablename)) {
    return new Response(JSON.stringify({ error: 'Invalid table name' }), { status: 400 });
  }

  // Sökvägen till DuckDB‑filen (relativt från evidence‑mappen)
  const dbPath = './../dbt_project/data/nerd_facts.duckdb';

  // Skapa en anslutning
  const db = new duckdb.Database(dbPath, {
    access_mode: "READ_ONLY"
} );

  // Bygg SQL-frågan – observera att du bör sanera indata ordentligt
  const sql = `SELECT * FROM ${schema}.${tablename}`;

  // Returnera resultatet som JSON
  return new Promise((resolve, reject) => {
    db.all(sql, (err, rows) => {
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
