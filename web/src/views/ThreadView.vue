<script setup lang="ts">
import { computed, nextTick, onMounted, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { useI18n } from "vue-i18n";
import { api } from "@/lib/api";
import { streamChatAnswer } from "@/lib/stream";
import { useResearchStore } from "@/stores/research";
import ResearchTurn from "@/components/ResearchTurn.vue";
import Composer from "@/components/Composer.vue";
import MarkdownView from "@/components/MarkdownView.vue";
import type { Depth } from "@/lib/types";

// A conversation thread: a sequence of deep researches and quick grounded
// follow-up questions, with one composer (mode toggle) at the bottom.
type ResearchItem = { kind: "research"; id: string; prompt: string };
type ChatItem = { kind: "chat"; question: string; answer: string; researchId: string; busy: boolean; searching: boolean };
type ThreadItem = ResearchItem | ChatItem;

const props = defineProps<{ threadId: string }>();
const router = useRouter();
const store = useResearchStore();
const { t } = useI18n();

const items = ref<ThreadItem[]>([]);
const composerPrompt = ref("");
const busy = ref(false);
const errorMsg = ref<string | null>(null);
const scroller = ref<HTMLElement | null>(null);
const completed = ref<Set<string>>(new Set());

const threadTitle = computed(() => {
  const first = items.value.find((it) => it.kind === "research") as ResearchItem | undefined;
  return first?.prompt ?? "";
});

// Quick questions are grounded on the most recent completed research in the thread.
const latestCompletedResearchId = computed(() => {
  for (let i = items.value.length - 1; i >= 0; i--) {
    const it = items.value[i];
    if (it.kind === "research" && completed.value.has(it.id)) return it.id;
  }
  return null;
});

async function scrollToBottom() {
  await nextTick();
  scroller.value?.scrollTo({ top: scroller.value.scrollHeight, behavior: "smooth" });
}

async function loadThread() {
  try {
    const list = await api.getThread(props.threadId);
    items.value = list.map((r) => ({ kind: "research", id: r.id, prompt: r.prompt }));
  } catch (e) {
    errorMsg.value = (e as Error).message;
  }
}

function onTurnDone(id: string, status: string) {
  if (status === "completed") completed.value = new Set(completed.value).add(id);
}

async function onSubmit(payload: { prompt: string; depth: Depth; model: string; planFirst: boolean }) {
  busy.value = true;
  errorMsg.value = null;
  try {
    const res = await store.createResearch(
      payload.prompt, payload.depth, payload.model, payload.planFirst, props.threadId,
    );
    items.value.push({ kind: "research", id: res.research_id, prompt: payload.prompt });
    composerPrompt.value = "";
    await scrollToBottom();
  } catch (e) {
    errorMsg.value = (e as Error).message;
  } finally {
    busy.value = false;
  }
}

async function onAsk(question: string) {
  const researchId = latestCompletedResearchId.value;
  if (!researchId) {
    errorMsg.value = t("thread.needResearch");
    return;
  }
  errorMsg.value = null;
  const idx = items.value.push({
    kind: "chat", question, answer: "", researchId, busy: true, searching: false,
  }) - 1;
  const chat = () => items.value[idx] as ChatItem; // mutate through the reactive proxy
  composerPrompt.value = "";
  await scrollToBottom();
  await streamChatAnswer(researchId, question, {
    onSearching: () => { chat().searching = true; },
    onDelta: (a) => { const c = chat(); c.searching = false; c.answer = a; scrollToBottom(); },
    onDone: (a) => { const c = chat(); c.searching = false; c.answer = a; c.busy = false; scrollToBottom(); },
    onError: (m) => { chat().busy = false; errorMsg.value = m; },
  });
}

onMounted(loadThread);
watch(() => props.threadId, () => { completed.value = new Set(); loadThread(); });
</script>

<template>
  <div class="flex h-full flex-col">
    <header class="flex items-center gap-3 border-b border-bd px-4 py-3">
      <button class="text-sm text-muted hover:text-ink" @click="router.push('/')">
        {{ $t("common.back") }}
      </button>
      <span v-if="threadTitle" class="truncate text-sm text-muted">{{ threadTitle }}</span>
    </header>

    <div ref="scroller" class="min-h-0 flex-1 overflow-y-auto px-4 py-6">
      <div class="mx-auto max-w-3xl space-y-10">
        <template v-for="(it, i) in items" :key="i">
          <ResearchTurn
            v-if="it.kind === 'research'"
            :id="it.id"
            :initial-prompt="it.prompt"
            @done="onTurnDone(it.id, $event)"
          />
          <div v-else class="space-y-3">
            <div class="flex justify-end">
              <div class="max-w-[80%] whitespace-pre-wrap rounded-2xl bg-surface px-4 py-2.5 text-[15px] text-ink">
                {{ it.question }}
              </div>
            </div>
            <MarkdownView v-if="it.answer" :source="it.answer" />
            <div v-if="it.busy && !it.answer" class="flex items-center gap-2 text-sm text-muted">
              <span class="h-1.5 w-1.5 animate-pulse rounded-full bg-accent" />
              {{ it.searching ? $t("chat.searching") : $t("common.thinking") }}
            </div>
          </div>
        </template>

        <p v-if="errorMsg" class="text-sm text-red-400">{{ errorMsg }}</p>
        <p v-if="!items.length && !errorMsg" class="text-muted">{{ $t("thread.empty") }}</p>
      </div>
    </div>

    <div class="shrink-0 border-t border-bd p-4">
      <div class="mx-auto flex max-w-3xl justify-center">
        <Composer
          v-model:prompt="composerPrompt"
          :busy="busy"
          allow-quick-question
          @submit="onSubmit"
          @ask="onAsk"
        />
      </div>
    </div>
  </div>
</template>
