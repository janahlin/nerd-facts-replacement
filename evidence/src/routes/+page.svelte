<!-- evidence/src/routes/+page.svelte -->
<script>
  import { onMount } from 'svelte';
  import { writable } from 'svelte/store';

  // Lista med tabellval
  const tables = [
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
  let selectedTable = tables[0]; // förvalt val

  const data = writable([]);
  const error = writable(null);

  async function fetchTableData() {
    error.set(null);
    try {      
      const res = await fetch(`/api/table/${selectedTable}`);
      if (!res.ok) {        
        throw new Error(`Fel vid hämtning: ${res.status}`);
      }
      const json = await res.json();
      data.set(json);
    } catch (e) {
      error.set(e);
      console.error(e);
    }
  }

  // Hämta data initialt
  onMount(fetchTableData);
</script>

<h1>Interaktiv Dashboard</h1>

<label for="table-select">Välj tabell:</label>
<select id="table-select" bind:value={selectedTable} on:change={fetchTableData}>
  {#each tables as table}
    <option value={table}>{table}</option>
  {/each}
</select>

{#if $error}
  <p style="color: red;">{$error.message}</p>
{/if}

{#if $data && $data.length > 0}
  <table border="1" cellpadding="5">
    <thead>
      <tr>
        <!-- Dynamiskt skapa kolumnrubriker från nycklarna i första raden -->
        {#each Object.keys($data[0]) as key}
          <th>{key}</th>
        {/each}
      </tr>
    </thead>
    <tbody>
      {#each $data as row}
        <tr>
          {#each Object.values(row) as value}
            <td>{value}</td>
          {/each}
        </tr>
      {/each}
    </tbody>
  </table>
{:else}
  <p>Inga data att visa för {selectedTable}.</p>
{/if}
