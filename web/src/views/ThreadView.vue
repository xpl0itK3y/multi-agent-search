<script setup lang="ts">
import { computed, nextTick, onMounted, ref, watch } from "vue";
import { useI18n } from "vue-i18n";
import { api, apiErrorMessage } from "@/lib/api";
import { streamChatAnswer } from "@/lib/stream";
import { useStickToBottom } from "@/lib/useStickToBottom";
import { useResearchStore } from "@/stores/research";
import ResearchTurn from "@/components/ResearchTurn.vue";
import Composer from "@/components/Composer.vue";
import MarkdownView from "@/components/MarkdownView.vue";
import type { ChatMessage, Depth } from "@/lib/types";

// A conversation thread: a sequence of deep researches and quick grounded
// follow-up questions, with one composer (mode toggle) at the bottom.
type ResearchItem = { kind: "research"; id: string; prompt: string };
type ChatItem = {
  kind: "chat";
  question: string;
  answer: string;
  sources: ChatMessage["sources"];
  researchId: string;
  busy: boolean;
  searching: boolean;
  // A failed answer stays with its question, with a retry (feedback near its cause).
  error: string | null;
};
type ThreadItem = ResearchItem | ChatItem;

const props = defineProps<{ threadId: string }>();
const store = useResearchStore();
const { t } = useI18n();

const items = ref<ThreadItem[]>([]);
const composerPrompt = ref("");
const busy = ref(false);
const errorMsg = ref<string | null>(null);
// Until the thread has loaded, show its shape, never the "nothing here" sentence.
const loading = ref(true);
const scroller = ref<HTMLElement | null>(null);
const completed = ref<Set<string>>(new Set());
const finished = ref<Set<string>>(new Set());

// A research is "running" until it reaches a terminal state — block new ones meanwhile.
const anyResearchRunning = computed(() =>
  items.value.some((it) => it.kind === "research" && !finished.value.has(it.id)),
);

// Quick questions are grounded on the most recent completed research; fall back to the
// last research turn so a visible report can always be questioned (the done-event may
// not have registered after a reload).
const latestCompletedResearchId = computed(() => {
  let lastResearch: string | null = null;
  for (let i = items.value.length - 1; i >= 0; i--) {
    const it = items.value[i];
    if (it.kind !== "research") continue;
    if (lastResearch === null) lastResearch = it.id;
    if (completed.value.has(it.id)) return it.id;
  }
  return lastResearch;
});

// Quick questions need a finished report to ground on — only allow once one has completed.
const hasCompletedReport = computed(() => completed.value.size > 0);

// Streamed turns follow the newest content only while the reader stays at the bottom;
// scrolling up to read lets go, and the pill brings them back (apple-design §3).
const stick = useStickToBottom(scroller);
const showToLatest = computed(
  () => !stick.pinned.value && (anyResearchRunning.value || items.value.some((it) => it.kind === "chat" && it.busy)),
);

// Pair a flat [user, assistant, user, assistant, …] message log into Q&A turns,
// tolerating an unanswered trailing question or a missing question.
function pairMessages(msgs: ChatMessage[]): { question: string; answer: string; sources: ChatMessage["sources"] }[] {
  const pairs: { question: string; answer: string; sources: ChatMessage["sources"] }[] = [];
  let pendingQ: string | null = null;
  for (const m of msgs) {
    if (m.role === "user") {
      if (pendingQ !== null) pairs.push({ question: pendingQ, answer: "", sources: [] });
      pendingQ = m.content;
    } else {
      pairs.push({ question: pendingQ ?? "", answer: m.content, sources: m.sources || [] });
      pendingQ = null;
    }
  }
  if (pendingQ !== null) pairs.push({ question: pendingQ, answer: "", sources: [] });
  return pairs;
}

// The tab says which thread this is (wayfinding): its first question, shortened.
const TITLE_MAX = 60;
function threadTitle(prompt: string): string {
  const text = prompt.replace(/\s+/g, " ").trim();
  const chars = Array.from(text);
  return chars.length > TITLE_MAX ? `${chars.slice(0, TITLE_MAX - 1).join("").trimEnd()}…` : text;
}

// Only the latest load may fill the view: switching threads mid-load drops the older one.
let loadSeq = 0;

async function loadThread() {
  const seq = ++loadSeq;
  loading.value = true;
  try {
    const list = await api.getThread(props.threadId);
    // Load each research's saved Q&A in parallel, then interleave so follow-up
    // questions reappear under their research after a refresh / re-login.
    const messages = await Promise.all(list.map((r) => api.getMessages(r.id).catch(() => [])));
    if (seq !== loadSeq) return;
    const built: ThreadItem[] = [];
    list.forEach((r, idx) => {
      built.push({ kind: "research", id: r.id, prompt: r.prompt });
      for (const p of pairMessages(messages[idx])) {
        built.push({
          kind: "chat", question: p.question, answer: p.answer, sources: p.sources, researchId: r.id,
          busy: false, searching: false, error: null,
        });
      }
    });
    items.value = built;
    const first = list[0]?.prompt ? threadTitle(list[0].prompt) : "";
    if (first) document.title = `${first} — Veris`;
  } catch (e) {
    if (seq === loadSeq) errorMsg.value = apiErrorMessage(e, t);
  } finally {
    if (seq === loadSeq) loading.value = false;
  }
}

function onTurnDone(id: string, status: string) {
  finished.value = new Set(finished.value).add(id);
  if (status === "completed") completed.value = new Set(completed.value).add(id);
}

// "Refresh" cloned the research into a new run — add it as a new turn in the thread.
async function onRefreshed(payload: { id: string; prompt: string }) {
  items.value.push({ kind: "research", id: payload.id, prompt: payload.prompt });
  await nextTick();
  stick.jumpToLatest();
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
    await nextTick();
    stick.jumpToLatest();
  } catch (e) {
    errorMsg.value = apiErrorMessage(e, t);
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
    kind: "chat", question, answer: "", sources: [], researchId, busy: true, searching: false, error: null,
  }) - 1;
  composerPrompt.value = "";
  await nextTick();
  stick.jumpToLatest();
  await streamInto(idx);
}

// Stream the answer to the question at items[idx] into that same item.
async function streamInto(idx: number) {
  // The reactive proxy of this item: after a thread switch it is detached, so a late
  // token can't land in another thread's question.
  const c = items.value[idx] as ChatItem;
  await streamChatAnswer(c.researchId, c.question, {
    onSearching: () => { c.searching = true; },
    onDelta: (a) => { c.searching = false; c.answer = a; stick.follow(); },
    onDone: (a, sources) => { c.searching = false; c.answer = a; c.sources = sources; c.busy = false; stick.follow(); },
    onError: (m) => { c.searching = false; c.busy = false; c.error = m; },
  });
}

function retryAsk(idx: number) {
  const c = items.value[idx] as ChatItem;
  c.error = null;
  c.answer = "";
  c.sources = [];
  c.busy = true;
  streamInto(idx);
}

onMounted(loadThread);
watch(() => props.threadId, () => {
  stick.pinned.value = true; // a newly opened thread starts at its latest turn
  items.value = [];
  errorMsg.value = null;
  completed.value = new Set();
  finished.value = new Set();
  loading.value = true;
  loadThread();
});
</script>

<template>
  <div class="relative flex h-full flex-col">
    <div ref="scroller" class="edge-fade-top min-h-0 flex-1 overflow-y-auto px-4 pt-6 pb-48">
      <div class="mx-auto max-w-3xl space-y-10">
        <!-- Loading: the thread's shape (a question, a turn), never "this thread is empty". -->
        <div v-if="loading" class="space-y-4" aria-busy="true" role="status">
          <span class="sr-only">{{ $t("common.loading") }}</span>
          <div class="flex justify-end"><div class="h-12 w-3/5 rounded-2xl bg-surface animate-pulse" /></div>
          <div class="h-40 rounded-xl border border-bd bg-surface/60 animate-pulse" />
        </div>

        <template v-for="(it, i) in items" :key="it.kind === 'research' ? it.id : `q${i}`">
          <ResearchTurn
            v-if="it.kind === 'research'"
            :id="it.id"
            :initial-prompt="it.prompt"
            @done="onTurnDone(it.id, $event)"
            @refreshed="onRefreshed"
            @grow="stick.follow()"
          />
          <div v-else class="space-y-3">
            <div class="flex justify-end">
              <div class="max-w-[80%] whitespace-pre-wrap rounded-2xl bg-surface px-4 py-2.5 text-[15px] text-ink">
                {{ it.question }}
              </div>
            </div>
            <MarkdownView v-if="it.answer" :source="it.answer" :sources="it.sources || []" />
            <div v-if="it.busy && !it.answer" class="flex items-center gap-2 text-sm text-muted">
              <span class="live-dot h-1.5 w-1.5 rounded-full bg-accent" />
              {{ it.searching ? $t("chat.searching") : $t("common.thinking") }}
            </div>
            <p v-if="it.error" role="alert" class="text-sm text-danger">
              {{ it.error }}
              <button type="button" class="press ml-2 text-accent hover:underline" @click="retryAsk(i)">
                {{ $t("research.retry") }}
              </button>
            </p>
          </div>
        </template>

        <p v-if="errorMsg" class="text-sm text-danger">{{ errorMsg }}</p>
        <p v-if="!loading && !items.length && !errorMsg" class="text-muted">{{ $t("thread.empty") }}</p>
      </div>
    </div>

    <div class="pointer-events-none absolute inset-x-0 bottom-0 bg-gradient-to-t from-bg via-bg/70 to-transparent p-4 pt-10">
      <Transition name="fade-quick">
        <button
          v-if="showToLatest"
          type="button"
          class="press pointer-events-auto mx-auto mb-2 flex w-fit items-center gap-1.5 rounded-full border border-bd material-float px-3 py-1 text-xs font-medium text-ink"
          @click="stick.jumpToLatest()"
        >
          <span aria-hidden="true">↓</span> {{ $t("thread.toLatest") }}
        </button>
      </Transition>
      <div class="pointer-events-auto mx-auto flex max-w-3xl justify-center">
        <Composer
          v-model:prompt="composerPrompt"
          :busy="busy"
          :research-busy="anyResearchRunning"
          :allow-quick-question="hasCompletedReport"
          @submit="onSubmit"
          @ask="onAsk"
        />
      </div>
    </div>
  </div>
</template>
