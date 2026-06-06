<script setup lang="ts">
import { computed, ref } from "vue";
import { useRouter } from "vue-router";
import SparkLogo from "@/components/SparkLogo.vue";
import Composer from "@/components/Composer.vue";
import SuggestionChips from "@/components/SuggestionChips.vue";
import { useResearchStore } from "@/stores/research";
import { useUiStore } from "@/stores/ui";
import type { Depth } from "@/lib/types";

const router = useRouter();
const store = useResearchStore();
const ui = useUiStore();

const prompt = ref("");
const busy = ref(false);
const errorMsg = ref<string | null>(null);

const greeting = computed(() => {
  const h = new Date().getHours();
  const part =
    h < 6 ? "Доброй ночи" : h < 12 ? "Доброе утро" : h < 18 ? "Добрый день" : "Добрый вечер";
  return `${part}, ${ui.userName}`;
});

async function onSubmit(payload: { prompt: string; depth: Depth; model: string }) {
  busy.value = true;
  errorMsg.value = null;
  try {
    const id = await store.createResearch(payload.prompt, payload.depth, payload.model);
    router.push({ name: "research", params: { id } });
  } catch (e) {
    errorMsg.value = (e as Error).message;
  } finally {
    busy.value = false;
  }
}

function onPick(template: string) {
  prompt.value = template;
}
</script>

<template>
  <div class="flex h-full flex-col items-center justify-center overflow-y-auto px-6 py-10">
    <div class="mb-8 flex items-center gap-3">
      <SparkLogo :size="34" />
      <h1 class="font-serif text-4xl font-medium tracking-tight text-ink">{{ greeting }}</h1>
    </div>

    <Composer v-model:prompt="prompt" :busy="busy" class="mb-4" @submit="onSubmit" />

    <p v-if="errorMsg" class="mb-3 text-sm text-red-400">{{ errorMsg }}</p>

    <SuggestionChips @pick="onPick" />
  </div>
</template>
