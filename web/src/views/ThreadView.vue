<script setup lang="ts">
import { nextTick, onMounted, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { api } from "@/lib/api";
import { useResearchStore } from "@/stores/research";
import ResearchTurn from "@/components/ResearchTurn.vue";
import Composer from "@/components/Composer.vue";
import type { Depth } from "@/lib/types";

// A conversation thread: multiple deep researches in one scroll, with a composer
// at the bottom to start another (e.g. "didn't like it — redo with more depth").
const props = defineProps<{ threadId: string }>();
const router = useRouter();
const store = useResearchStore();

const turns = ref<{ id: string; prompt: string }[]>([]);
const composerPrompt = ref("");
const busy = ref(false);
const errorMsg = ref<string | null>(null);
const scroller = ref<HTMLElement | null>(null);

async function scrollToBottom() {
  await nextTick();
  scroller.value?.scrollTo({ top: scroller.value.scrollHeight, behavior: "smooth" });
}

async function loadThread() {
  try {
    const items = await api.getThread(props.threadId);
    turns.value = items.map((r) => ({ id: r.id, prompt: r.prompt }));
  } catch (e) {
    errorMsg.value = (e as Error).message;
  }
}

async function onSubmit(payload: { prompt: string; depth: Depth; model: string; planFirst: boolean }) {
  busy.value = true;
  errorMsg.value = null;
  try {
    const res = await store.createResearch(
      payload.prompt,
      payload.depth,
      payload.model,
      payload.planFirst,
      props.threadId,
    );
    turns.value.push({ id: res.research_id, prompt: payload.prompt });
    composerPrompt.value = "";
    await scrollToBottom();
  } catch (e) {
    errorMsg.value = (e as Error).message;
  } finally {
    busy.value = false;
  }
}

onMounted(loadThread);
// Switching threads from the sidebar reuses this component — reload on id change.
watch(() => props.threadId, loadThread);
</script>

<template>
  <div class="flex h-full flex-col">
    <header class="flex items-center gap-3 border-b border-bd px-4 py-3">
      <button class="text-sm text-muted hover:text-ink" @click="router.push('/')">
        {{ $t("common.back") }}
      </button>
      <span v-if="turns.length" class="truncate text-sm text-muted">{{ turns[0].prompt }}</span>
    </header>

    <div ref="scroller" class="min-h-0 flex-1 overflow-y-auto px-4 py-6">
      <div class="mx-auto max-w-3xl space-y-10">
        <ResearchTurn v-for="turn in turns" :key="turn.id" :id="turn.id" :initial-prompt="turn.prompt" />
        <p v-if="errorMsg" class="text-sm text-red-400">{{ errorMsg }}</p>
        <p v-if="!turns.length && !errorMsg" class="text-muted">{{ $t("thread.empty") }}</p>
      </div>
    </div>

    <div class="shrink-0 border-t border-bd p-4">
      <div class="mx-auto flex max-w-3xl justify-center">
        <Composer v-model:prompt="composerPrompt" :busy="busy" @submit="onSubmit" />
      </div>
    </div>
  </div>
</template>
