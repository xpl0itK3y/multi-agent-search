<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { useI18n } from "vue-i18n";
import SparkLogo from "@/components/SparkLogo.vue";
import Composer from "@/components/Composer.vue";
import SuggestionChips from "@/components/SuggestionChips.vue";
import PromptExamples from "@/components/PromptExamples.vue";
import { useResearchStore } from "@/stores/research";
import { useUiStore } from "@/stores/ui";
import { useAuthStore } from "@/stores/auth";
import { apiErrorMessage } from "@/lib/api";
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
    errorMsg.value = apiErrorMessage(e, t);
  } finally {
    busy.value = false;
  }
}

// Caret at the end of the text, ready to type on.
function focusComposer() {
  nextTick(() => composerRef.value?.focus());
}

// A chip is a prefix ("Compare and pick the best: "): it goes in front of what was typed
// instead of wiping it, and another chip swaps only the prefix (apple-design §16 agency).
const CHIP_KEYS = ["market", "compare", "lit", "url"] as const;
const chipTemplates = computed(() => CHIP_KEYS.map((k) => t(`chips.${k}.template`)));
function onChip(template: string) {
  const current = prompt.value;
  const prefix = chipTemplates.value.find((p) => current.startsWith(p));
  if (!current.trim()) prompt.value = template;
  else if (prefix) prompt.value = template + current.slice(prefix.length);
  else prompt.value = template + current.trim();
  focusComposer();
}

// An example replaces the text, so what was typed can be brought back (forgiveness: an
// undo for the slip, not a confirmation). The offer goes away on restore, on the next
// edit, or after a while.
const RESTORE_MS = 8000;
const previousPrompt = ref<string | null>(null);
let exampleText = "";
let restoreTimer: ReturnType<typeof setTimeout> | undefined;
function hideRestore() {
  clearTimeout(restoreTimer);
  previousPrompt.value = null;
}
function onExample(text: string) {
  const current = prompt.value;
  if (!current.trim() || current === text) {
    hideRestore();
  } else {
    // Another example while the offer is up still restores the text typed before the first.
    if (previousPrompt.value === null || current !== exampleText) previousPrompt.value = current;
    exampleText = text;
    clearTimeout(restoreTimer);
    restoreTimer = setTimeout(hideRestore, RESTORE_MS);
  }
  prompt.value = text;
  focusComposer();
}
function restorePrompt() {
  if (previousPrompt.value === null) return;
  const text = previousPrompt.value;
  hideRestore();
  prompt.value = text;
  focusComposer();
}
watch(prompt, (value) => {
  if (previousPrompt.value !== null && value !== exampleText) hideRestore();
});
onBeforeUnmount(() => clearTimeout(restoreTimer));
</script>

<template>
  <div class="flex h-full flex-col items-center overflow-y-auto px-4 py-8 sm:px-6">
    <!-- Centred with an auto margin inside the scroller: on a short screen the greeting
         stays reachable instead of being pushed above the top. -->
    <div class="my-auto flex w-full flex-col items-center">
      <div class="mb-8 flex items-center gap-3">
        <SparkLogo :size="34" />
        <h1 class="text-balance font-serif text-3xl font-medium tracking-tight text-ink sm:text-4xl">{{ greeting }}</h1>
      </div>

      <Composer ref="composerRef" v-model:prompt="prompt" :busy="busy" class="mb-4" @submit="onSubmit" />

      <!-- Zero height: the offer sits in the gap under the composer, so nothing below moves
           under the pointer that just picked an example. -->
      <div class="relative w-full max-w-composer">
        <Transition name="fade-quick">
          <button
            v-if="previousPrompt !== null"
            type="button"
            class="press absolute -top-3.5 right-4 text-xs text-accent hover:underline"
            @click="restorePrompt"
          >
            {{ $t("home.restorePrompt") }}
          </button>
        </Transition>
      </div>

      <p v-if="errorMsg" role="alert" class="mb-3 text-sm text-danger">{{ errorMsg }}</p>

      <SuggestionChips @pick="onChip" />

      <PromptExamples class="mt-8" @pick="onExample" />
    </div>
  </div>
</template>
