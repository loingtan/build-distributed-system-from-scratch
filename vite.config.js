import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { labPlugin } from './lab/server.mjs';

export default defineConfig({
  base: './',
  // labPlugin serves /api/* so the page can build and run Go nodes as real processes.
  plugins: [react(), labPlugin()],
  worker: { format: 'es' },
  server: { watch: { ignored: ['**/practice/**', '**/.lab/**'] } },
});
