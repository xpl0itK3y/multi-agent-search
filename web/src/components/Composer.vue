<script setup lang="ts">
import { computed, nextTick, onMounted, ref, watch } from "vue";
import { useResearchStore } from "@/stores/research";
import type { Depth } from "@/lib/types";

const prompt = defineModel<string>("prompt", { default: "" });

// Auto-grow the textarea with its content (capped); the user can also drag-resize it.
const textarea = ref<HTMLTextAreaElement | null>(null);
const MAX_TEXTAREA_PX = 360;
function autosize() {
  const el = textarea.value;
  if (!el) return;
  el.style.height = "auto";
  el.style.height = Math.min(el.scrollHeight, MAX_TEXTAREA_PX) + "px";
}
watch(prompt, () => nextTick(autosize));
onMounted(autosize);

const props = defineProps<{ busy?: boolean; allowQuickQuestion?: boolean; researchBusy?: boolean }>();
const emit = defineEmits<{
  submit: [{ prompt: string; depth: Depth; model: string; planFirst: boolean }];
  ask: [string];
}>();

const store = useResearchStore();

const depths: Depth[] = ["easy", "medium", "hard"];
const depth = ref<Depth>("medium");
const planFirst = ref(true);

// Thread composer can switch between starting a deep research and a quick grounded
// follow-up question on the latest report.
const mode = ref<"research" | "quick">("research");
const isQuick = computed(() => !!props.allowQuickQuestion && mode.value === "quick");
// Fall back to research mode whenever quick-questions aren't available (no finished report yet).
watch(() => props.allowQuickQuestion, (allowed) => { if (!allowed) mode.value = "research"; });

const model = ref<string>("");
watch(
  () => store.defaultModelId,
  (id) => {
    if (!model.value && id) model.value = id;
  },
  { immediate: true },
);

// Block a new deep research while one is still running (system guard); quick
// questions stay allowed since they're cheap.
const researchBlocked = computed(() => !isQuick.value && !!props.researchBusy);
const canSubmit = computed(() => {
  const len = prompt.value.trim().length;
  if (props.busy || researchBlocked.value) return false;
  return isQuick.value ? len >= 1 : len >= 5;
});

function submit() {
  if (!canSubmit.value) return;
  if (isQuick.value) {
    emit("ask", prompt.value.trim());
    return;
  }
  emit("submit", {
    prompt: prompt.value.trim(),
    depth: depth.value,
    model: model.value,
    planFirst: planFirst.value,
  });
}

function onKeydown(e: KeyboardEvent) {
  if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
    e.preventDefault();
    submit();
  }
}
</script>

<template>
  <div class="w-full max-w-composer rounded-card border border-bd bg-surface px-4 py-3 shadow-lg">
    <textarea
      ref="textarea"
      v-model="prompt"
      rows="2"
      :placeholder="isQuick ? $t('composer.quickPlaceholder') : $t('composer.placeholder')"
      class="block w-full resize-y overflow-y-auto bg-transparent text-[15px] leading-relaxed text-ink placeholder:text-muted focus:outline-none max-h-[360px]"
      @input="autosize"
      @keydown="onKeydown"
    />

    <p v-if="researchBlocked" class="mt-1 text-xs text-muted">{{ $t("composer.researchBusyHint") }}</p>

    <div class="mt-2 flex items-center justify-between gap-2">
      <div class="flex items-center gap-2">
        <!-- Research / quick-question mode toggle -->
        <div v-if="allowQuickQuestion" class="flex items-center rounded-full border border-bd p-0.5 text-xs">
          <button
            class="rounded-full px-2.5 py-1 transition"
            :class="mode === 'research' ? 'bg-accent/15 text-accentSoft' : 'text-muted hover:text-ink'"
            @click="mode = 'research'"
          >
            {{ $t("composer.modeResearch") }}
          </button>
          <button
            class="rounded-full px-2.5 py-1 transition"
            :class="mode === 'quick' ? 'bg-accent/15 text-accentSoft' : 'text-muted hover:text-ink'"
            @click="mode = 'quick'"
          >
            {{ $t("composer.modeQuick") }}
          </button>
        </div>

        <button v-if="!isQuick" class="grid h-8 w-8 place-items-center rounded-full border border-bd text-muted hover:text-ink" :title="$t('composer.attach')">
          +
        </button>
        <button
          v-if="!isQuick"
          class="flex items-center gap-1.5 rounded-full border px-3 py-1.5 text-sm transition"
          :class="planFirst ? 'border-accent/40 bg-accent/15 text-accentSoft' : 'border-bd text-muted hover:text-ink'"
          :title="planFirst ? $t('composer.planOn') : $t('composer.planOff')"
          @click="planFirst = !planFirst"
        >
          <span>◳</span> {{ $t("composer.plan") }}
        </button>
      </div>

      <div class="flex items-center gap-2">
        <!-- Model selector -->
        <select
          v-if="!isQuick"
          v-model="model"
          class="cursor-pointer rounded-lg border border-bd bg-transparent px-2 py-1.5 text-sm text-muted hover:text-ink focus:outline-none"
          :title="$t('composer.model')"
        >
          <option v-if="!store.models.length" :value="''">{{ $t("composer.model") }}</option>
          <option v-for="m in store.models" :key="m.id" :value="m.id">{{ m.label }}</option>
        </select>

        <!-- Depth selector -->
        <select
          v-if="!isQuick"
          v-model="depth"
          class="cursor-pointer rounded-lg border border-bd bg-transparent px-2 py-1.5 text-sm text-muted hover:text-ink focus:outline-none"
          :title="$t('composer.depth')"
        >
          <option v-for="d in depths" :key="d" :value="d">{{ $t("depth." + d) }}</option>
        </select>

        <!-- Send -->
        <button
          class="grid h-8 w-8 place-items-center rounded-full bg-accent text-bg transition disabled:cursor-not-allowed disabled:opacity-40"
          :disabled="!canSubmit"
          :title="$t('composer.runTitle')"
          @click="submit"
        >
          <span v-if="props.busy" class="animate-pulse">…</span>
          <span v-else>↑</span>
        </button>
      </div>
    </div>
  </div>
</template>
