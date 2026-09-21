<script setup lang="ts">
import { computed, nextTick, ref } from "vue";
import { useRouter } from "vue-router";
import { useI18n } from "vue-i18n";
import SparkLogo from "@/components/SparkLogo.vue";
import Composer from "@/components/Composer.vue";
import SuggestionChips from "@/components/SuggestionChips.vue";
import PromptExamples from "@/components/PromptExamples.vue";
import { useResearchStore } from "@/stores/research";
import { useUiStore } from "@/stores/ui";
import { useAuthStore } from "@/stores/auth";
import type { Depth } from "@/lib/types";

const router = useRouter();
const store = useResearchStore();
const ui = useUiStore();
const auth = useAuthStore();
const { t } = useI18n();

const composerRef = ref<InstanceType<typeof Composer> | null>(null);
const prompt = ref("");
const busy = ref(false);
const errorMsg = ref<string | null>(null);

const greetingName = computed(() => {
  const user = auth.user;
  if (!user) return ui.userName || "";
  const rawName = user.name?.trim();
  if (rawName) {
    return rawName.split(/\s+/)[0];
  }
  if (user.email) {
    return user.email.split("@")[0];
  }
  return ui.userName || "";
});

const greeting = computed(() => {
  const h = new Date().getHours();
  const partKey = h < 6 ? "night" : h < 12 ? "morning" : h < 18 ? "day" : "evening";
  const name = greetingName.value;
  if (!name) return t(`home.${partKey}`);
  return t("home.greeting", { part: t(`home.${partKey}`), name });
});

async function onSubmit(payload: { prompt: string; depth: Depth; model: string; planFirst: boolean }) {
  busy.value = true;
  errorMsg.value = null;
  try {
    const res = await store.createResearch(payload.prompt, payload.depth, payload.model, payload.planFirst);
    router.push({ name: "thread", params: { threadId: res.thread_id ?? res.research_id } });
  } catch (e) {
    errorMsg.value = (e as Error).message;
  } finally {
    busy.value = false;
  }
}

function onPick(template: string) {
  prompt.value = template;
  nextTick(() => {
    composerRef.value?.focus();
  });
}
</script>

<template>
  <div class="flex h-full flex-col items-center justify-center overflow-y-auto px-6 py-10">
    <div class="mb-8 flex items-center gap-3">
      <SparkLogo :size="34" />
      <h1 class="font-serif text-4xl font-medium tracking-tight text-ink">{{ greeting }}</h1>
    </div>

    <Composer ref="composerRef" v-model:prompt="prompt" :busy="busy" class="mb-4" @submit="onSubmit" />

    <p v-if="errorMsg" class="mb-3 text-sm text-red-400">{{ errorMsg }}</p>

    <SuggestionChips @pick="onPick" />

    <PromptExamples class="mt-8" @pick="onPick" />
  </div>
</template>
