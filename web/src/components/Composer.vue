<script setup lang="ts">
import { computed, nextTick, onMounted, ref, watch } from "vue";
import { useResearchStore } from "@/stores/research";
import type { Depth } from "@/lib/types";

const prompt = defineModel<string>("prompt", { default: "" });

// Auto-grow the textarea with its content (capped at 360px, then it scrolls). No manual
// resize handle: autosize reset any drag on the next keystroke.
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
const savedDepth = (typeof localStorage !== "undefined" ? localStorage.getItem("research.default_depth") : null) as Depth | null;
const depth = ref<Depth>(savedDepth && depths.includes(savedDepth) ? savedDepth : "medium");

const savedPlanFirst = typeof localStorage !== "undefined" ? localStorage.getItem("research.plan_first") : null;
const planFirst = ref<boolean>(savedPlanFirst !== null ? savedPlanFirst !== "false" : true);

// Thread composer can switch between starting a deep research and a quick grounded
// follow-up question on the latest report.
const mode = ref<"research" | "quick">("research");
const isQuick = computed(() => !!props.allowQuickQuestion && mode.value === "quick");
// Fall back to research mode whenever quick-questions aren't available (no finished report yet).
watch(() => props.allowQuickQuestion, (allowed) => { if (!allowed) mode.value = "research"; });

const savedModel = typeof localStorage !== "undefined" ? localStorage.getItem("research.default_model") : null;
const model = ref<string>(savedModel || "");
watch(
  () => store.defaultModelId,
  (id) => {
    if (!model.value && id) model.value = savedModel || id;
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

function focus() {
  if (textarea.value) {
    textarea.value.focus();
    const len = textarea.value.value.length;
    textarea.value.setSelectionRange(len, len);
  }
}

defineExpose({ focus });
</script>

<template>
  <div class="field-host material-float w-full max-w-composer rounded-card border border-bd px-4 py-3">
    <!-- One floating material: the composer is the lightest surface on the page, and the
         whole card shows focus (field-host) rather than a square ring inside it. -->
    <textarea
      ref="textarea"
      v-model="prompt"
      rows="2"
      :placeholder="isQuick ? $t('composer.quickPlaceholder') : $t('composer.placeholder')"
      class="field-bare block max-h-[360px] w-full resize-none overflow-y-auto bg-transparent text-[15px] leading-relaxed text-ink placeholder:text-muted focus:outline-none"
      @input="autosize"
      @keydown="onKeydown"
    />

    <p v-if="researchBlocked" class="mt-1 text-xs text-muted">{{ $t("composer.researchBusyHint") }}</p>

    <!-- Wraps on narrow screens: the selects and the send button take their own row. -->
    <div class="mt-2 flex flex-wrap items-center gap-2">
      <div class="flex min-w-0 items-center gap-2">
        <!-- Research / quick-question mode toggle -->
        <div v-if="allowQuickQuestion" class="flex items-center rounded-full border border-bd p-0.5 text-xs">
          <button
            type="button"
            class="press whitespace-nowrap rounded-full px-2.5 py-1"
            :class="mode === 'research' ? 'bg-accent/15 text-accentSoft' : 'text-muted hover:text-ink'"
            @click="mode = 'research'"
          >
            {{ $t("composer.modeResearch") }}
          </button>
          <button
            type="button"
            class="press whitespace-nowrap rounded-full px-2.5 py-1"
            :class="mode === 'quick' ? 'bg-accent/15 text-accentSoft' : 'text-muted hover:text-ink'"
            @click="mode = 'quick'"
          >
            {{ $t("composer.modeQuick") }}
          </button>
        </div>

        <button
          v-if="!isQuick"
          type="button"
          class="press flex items-center gap-1.5 whitespace-nowrap rounded-full border px-3 py-1.5 text-sm"
          :class="planFirst ? 'border-accent/40 bg-accent/15 text-accentSoft' : 'border-bd text-muted hover:text-ink'"
          :title="planFirst ? $t('composer.planOn') : $t('composer.planOff')"
          :aria-pressed="planFirst ? 'true' : 'false'"
          @click="planFirst = !planFirst"
        >
          <span aria-hidden="true">◳</span> {{ $t("composer.plan") }}
        </button>
      </div>

      <div class="ml-auto flex w-full min-w-0 items-center justify-end gap-2 sm:w-auto">
        <!-- Model selector (native: phones open their own picker) -->
        <select
          v-if="!isQuick"
          v-model="model"
          class="h-10 min-w-0 flex-1 cursor-pointer rounded-lg border border-bd bg-transparent px-2 py-1.5 text-sm text-muted hover:text-ink sm:h-auto sm:flex-none"
          :title="$t('composer.model')"
          :aria-label="$t('composer.model')"
        >
          <option v-if="!store.models.length" :value="''">{{ $t("composer.model") }}</option>
          <option v-for="m in store.models" :key="m.id" :value="m.id">{{ m.label }}</option>
        </select>

        <!-- Depth selector -->
        <select
          v-if="!isQuick"
          v-model="depth"
          class="h-10 min-w-0 flex-1 cursor-pointer rounded-lg border border-bd bg-transparent px-2 py-1.5 text-sm text-muted hover:text-ink sm:h-auto sm:flex-none"
          :title="$t('composer.depth')"
          :aria-label="$t('composer.depth')"
        >
          <option v-for="d in depths" :key="d" :value="d">{{ $t("depth." + d) }}</option>
        </select>

        <!-- Send: 40px on phones (plus the .hit padding), the .97 dip on press -->
        <button
          type="button"
          class="press hit grid h-10 w-10 shrink-0 place-items-center rounded-full bg-accent text-onAccent enabled:hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-40 sm:h-8 sm:w-8"
          :disabled="!canSubmit"
          :title="$t('composer.runTitle')"
          :aria-label="$t('composer.runTitle')"
          @click="submit"
        >
          <span v-if="props.busy" class="live-dot" aria-hidden="true">…</span>
          <span v-else aria-hidden="true">↑</span>
        </button>
      </div>
    </div>
  </div>
</template>
