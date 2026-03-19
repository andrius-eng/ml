import { defineConfig } from 'vite';

export default defineConfig({
  base: './',
  server: {
    port: 5173,
   proxy: {
     '/api': {
       target: 'http://localhost:8000',
       rewrite: (path) => path.replace(/^\/api/, ''),
     },
   },
  },
});
