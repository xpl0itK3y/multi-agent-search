// Empty base => same origin => Vite dev-proxy forwards /v1 to the backend.
// Set VITE_API_BASE (e.g. http://localhost:8000) for a non-proxied build.
const BASE = import.meta.env.VITE_API_BASE ?? "";
async function request(path, init) {
    const res = await fetch(`${BASE}${path}`, {
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        ...init,
    });
    if (!res.ok) {
        const detail = await res.text().catch(() => res.statusText);
        throw new Error(`${res.status} ${detail}`);
    }
    if (res.status === 204)
        return undefined;
    return (await res.json());
}
export const api = {
    listModels: () => request("/v1/models"),
    listResearch: (limit = 30) => request(`/v1/research?limit=${limit}`),
    createResearch: (body) => request("/v1/research", {
        method: "POST",
        body: JSON.stringify(body),
    }),
    getReport: (id) => request(`/v1/research/${id}/report`),
    deleteResearch: (id) => request(`/v1/research/${id}`, { method: "DELETE" }),
};
