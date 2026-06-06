import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import { fileURLToPath, URL } from "node:url";
// In dev we proxy API calls to the FastAPI backend so the SPA can use relative
// paths (and we don't even need CORS locally). Override the target with
// VITE_API_PROXY if your backend runs elsewhere.
const apiTarget = process.env.VITE_API_PROXY || "http://localhost:8000";
export default defineConfig({
    plugins: [vue()],
    resolve: {
        alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) },
    },
    server: {
        port: 5173,
        proxy: {
            "/v1": { target: apiTarget, changeOrigin: true },
            "/health": { target: apiTarget, changeOrigin: true },
        },
    },
});
