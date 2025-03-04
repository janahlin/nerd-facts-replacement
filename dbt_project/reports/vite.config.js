export default {
    server: {
      host: "0.0.0.0", // Ensure Vite binds to all network interfaces
      port: 3000, // Keep your Evidence default port
      strictPort: true, // Avoid random port changes
      allowedHosts: ["swapi.hartwigs.se"], // Add your Cloudflare Tunnel domain
    }
  };