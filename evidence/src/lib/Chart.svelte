<script>
    import { onMount } from 'svelte';
    import Chart from 'chart.js/auto';
    export let chartData;
    export let chartOptions = {};
    let canvas;
    
    onMount(() => {
      const ctx = canvas.getContext('2d');
      new Chart(ctx, {
        type: 'pie', // eller 'line', 'pie' etc.
        data: chartData,
        options: chartOptions
      });
    });
  </script>
  
  <canvas bind:this={canvas}></canvas>