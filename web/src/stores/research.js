import { defineStore } from "pinia";
import { computed, ref } from "vue";
import { api } from "@/lib/api";
export const useResearchStore = defineStore("research", () => {
    const models = ref([]);
    const history = ref([]);
    const loadingHistory = ref(false);
    const error = ref(null);
    const defaultModelId = computed(() => models.value.find((m) => m.default)?.id ?? models.value[0]?.id ?? "");
    async function fetchModels() {
        try {
            models.value = await api.listModels();
        }
        catch (e) {
            // Non-fatal: composer falls back to a single default option.
            error.value = e.message;
        }
    }
    async function fetchHistory() {
        loadingHistory.value = true;
        try {
            history.value = await api.listResearch(30);
        }
        catch (e) {
            error.value = e.message;
        }
        finally {
            loadingHistory.value = false;
        }
    }
    async function createResearch(prompt, depth, model) {
        const res = await api.createResearch({ prompt, depth, model });
        await fetchHistory();
        return res.research_id;
    }
    return {
        models,
        history,
        loadingHistory,
        error,
        defaultModelId,
        fetchModels,
        fetchHistory,
        createResearch,
    };
});
