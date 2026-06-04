import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In production the app is served as static files behind a gateway that routes
// /api/* to leaderboard-api. In dev, set VITE_API_BASE to a running
// leaderboard-api (e.g. http://localhost:8080) to use live data; leave it unset
// to run fully offline against built-in fixtures.
export default defineConfig({
  plugins: [react()],
  server: { host: true, port: 5173 },
});
