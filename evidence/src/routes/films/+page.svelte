<!-- evidence/src/routes/films/+page.svelte -->
<script>
  import { onMount } from 'svelte';
  import { writable } from 'svelte/store';
  import ChartComponent from '$lib/Chart.svelte';

  let films = writable([]);
  let error = writable(null);
  let fetchedData = [];  // Lokal variabel för rådata
  
  $: chartData = {
    labels: fetchedData.map(film => film.film_title),    
    datasets: [{
      label: 'Release Year',
      data: fetchedData.map(film => new Date(film.release_date).getFullYear()),
      backgroundColor: 'rgba(75, 192, 192, 0.6)'
    }]    
  };

  async function fetchFilms() {
    error.set(null);
    try {
      const res = await fetch('/api/table/films');
      if (!res.ok) throw new Error(`Error: ${res.status}`);
      const data = await res.json();      
      films.set(data);
      fetchedData = data; 
      console.log("Release yer:", fetchedData.map(film => new Date(film.release_date).getFullYear()))                       
    } catch (e) {
      error.set(e);
      console.error(e);
    }
  }

  onMount(fetchFilms);
</script>

<h1>Filmer</h1>

{#if $error}
  <p style="color: red;">{$error.message}</p>
{/if}

{#if $films && $films.length > 0}
  <table border="1" cellpadding="5">
    <thead>
      <tr>
        {#each Object.keys($films[0]) as key}
          <th>{key}</th>
        {/each}
      </tr>
    </thead>
    <tbody>
      {#each $films as film}
        <tr>
          {#each Object.values(film) as value}
            <td>{value}</td>
          {/each}
        </tr>
      {/each}
    </tbody>
  </table>
{:else}
  <p>Loading films...</p>
{/if}

<h2>Film Release Year Chart</h2>
{#key JSON.stringify(chartData)}
  <ChartComponent {chartData} />
{/key}

