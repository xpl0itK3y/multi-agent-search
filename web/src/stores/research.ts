import { defineStore } from "pinia";
import { computed, ref } from "vue";
import { api } from "@/lib/api";
import type { Depth, ModelOption, ResearchHistoryItem } from "@/lib/types";

export const useResearchStore = defineStore("research", () => {
  const models = ref<ModelOption[]>([]);
  const history = ref<ResearchHistoryItem[]>([]);
  const loadingHistory = ref(false);
  const error = ref<string | null>(null);

  const defaultModelId = computed(
    () => models.value.find((m) => m.default)?.id ?? models.value[0]?.id ?? "",
  );

  async function fetchModels() {
    try {
      models.value = await api.listModels();
    } catch (e) {
      // Non-fatal: composer falls back to a single default option.
      error.value = (e as Error).message;
    }
  }

  async function fetchHistory() {
    loadingHistory.value = true;
    try {
      history.value = await api.listResearch(30);
    } catch (e) {
      error.value = (e as Error).message;
    } finally {
      loadingHistory.value = false;
    }
  }

  async function createResearch(prompt: string, depth: Depth, model?: string, planFirst?: boolean) {
    const res = await api.createResearch({ prompt, depth, model, plan_first: planFirst });
    await fetchHistory();
    return res.research_id;
  }

  async function deleteResearch(id: string) {
    await api.deleteResearch(id);
    history.value = history.value.filter((h) => h.id !== id);
  }

  async function renameResearch(id: string, title: string) {
    await api.renameResearch(id, title);
    const item = history.value.find((h) => h.id === id);
    if (item) item.title = title;
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
    deleteResearch,
    renameResearch,
  };
});
