import adapter from '@sveltejs/adapter-auto';
import preprocess from 'svelte-preprocess';

/** @type {import('@sveltejs/kit').Config} */
const config = {
  preprocess: preprocess(),
  kit: {
    adapter: adapter(),
    files: {
      appTemplate: 'src/app.html',  // Säkerställer att SvelteKit använder din mall
      routes: 'src/routes'          // Se till att detta pekar på dina routes
    }
  }
};

export default config;
