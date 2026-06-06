<script setup lang="ts">
import { computed } from "vue";
import { useRoute, useRouter } from "vue-router";
import { useResearchStore } from "@/stores/research";
import { useUiStore } from "@/stores/ui";

const ui = useUiStore();
const store = useResearchStore();
const router = useRouter();
const route = useRoute();

const collapsed = computed(() => ui.sidebarCollapsed);

const nav = [
  { key: "researches", label: "Исследования", active: true },
  { key: "collections", label: "Коллекции", active: false },
  { key: "templates", label: "Шаблоны", active: false },
  { key: "customize", label: "Настроить", active: false },
];

function title(prompt: string): string {
  const t = prompt.trim().replace(/\s+/g, " ");
  return t.length > 30 ? t.slice(0, 30) + "…" : t;
}

function statusColor(status: string): string {
  if (status === "completed") return "bg-emerald-400";
  if (status === "failed") return "bg-red-400";
  if (status === "analyzing" || status === "processing") return "bg-accent";
  return "bg-muted";
}

function openResearch(id: string) {
  router.push({ name: "research", params: { id } });
}

const currentId = computed(() => (route.name === "research" ? route.params.id : null));
</script>

<template>
  <!-- Collapsed icon-rail -->
  <aside
    v-if="collapsed"
    class="flex h-full w-16 flex-col items-center gap-2 border-r border-bd bg-rail py-3"
  >
    <button class="rail-btn" title="Развернуть" @click="ui.toggleSidebar()">⌗</button>
    <button class="rail-btn" title="Новое исследование" @click="router.push('/')">+</button>
    <div class="mt-auto grid h-9 w-9 place-items-center rounded-full bg-surface text-sm">
      {{ ui.userName.charAt(0).toUpperCase() }}
    </div>
  </aside>

  <!-- Expanded sidebar -->
  <aside
    v-else
    class="flex h-full w-72 flex-col border-r border-bd bg-rail"
  >
    <!-- Brand -->
    <div class="flex items-center justify-between px-4 pt-4 pb-3">
      <span class="font-serif text-xl tracking-tight text-ink">Research</span>
      <div class="flex items-center gap-1 text-muted">
        <button class="icon-btn" title="Поиск">⌕</button>
        <button
          class="icon-btn"
          :title="ui.theme === 'dark' ? 'Светлая тема' : 'Тёмная тема'"
          @click="ui.toggleTheme()"
        >
          {{ ui.theme === "dark" ? "☀" : "☾" }}
        </button>
        <button class="icon-btn" title="Свернуть" @click="ui.toggleSidebar()">⌗</button>
      </div>
    </div>

    <!-- New research -->
    <div class="px-3">
      <button
        class="flex w-full items-center gap-2 rounded-lg px-3 py-2 text-sm text-ink hover:bg-surface"
        @click="router.push('/')"
      >
        <span class="grid h-6 w-6 place-items-center rounded-full border border-bd text-base leading-none">+</span>
        Новое исследование
      </button>
    </div>

    <!-- Primary nav -->
    <nav class="mt-1 px-3">
      <button
        v-for="item in nav"
        :key="item.key"
        class="flex w-full items-center gap-2 rounded-lg px-3 py-2 text-sm hover:bg-surface"
        :class="item.active ? 'bg-surface text-ink' : 'text-muted'"
      >
        {{ item.label }}
      </button>
    </nav>

    <!-- Recents -->
    <div class="mt-4 flex min-h-0 flex-1 flex-col">
      <div class="px-5 pb-1 text-xs uppercase tracking-wide text-muted">Недавние</div>
      <div class="min-h-0 flex-1 overflow-y-auto px-2">
        <p v-if="store.loadingHistory" class="px-3 py-2 text-sm text-muted">Загрузка…</p>
        <p v-else-if="!store.history.length" class="px-3 py-2 text-sm text-muted">
          Пока нет исследований.
        </p>
        <button
          v-for="item in store.history"
          :key="item.id"
          class="flex w-full items-center gap-2 truncate rounded-lg px-3 py-2 text-left text-sm hover:bg-surface"
          :class="currentId === item.id ? 'bg-surface text-ink' : 'text-muted'"
          @click="openResearch(item.id)"
        >
          <span class="h-1.5 w-1.5 shrink-0 rounded-full" :class="statusColor(item.status)" />
          <span class="truncate">{{ title(item.prompt) }}</span>
        </button>
      </div>
    </div>

    <!-- User card -->
    <div class="mt-auto flex items-center gap-3 border-t border-bd px-4 py-3">
      <div class="grid h-9 w-9 place-items-center rounded-full bg-surface text-sm">
        {{ ui.userName.charAt(0).toUpperCase() }}
      </div>
      <div class="min-w-0">
        <div class="truncate text-sm text-ink">{{ ui.userName }}</div>
        <div class="text-xs text-muted">Free plan</div>
      </div>
    </div>
  </aside>
</template>

<style scoped>
.rail-btn {
  display: grid;
  place-items: center;
  height: 2.25rem;
  width: 2.25rem;
  border-radius: 0.5rem;
  color: rgb(var(--c-muted));
}
.rail-btn:hover {
  background: rgb(var(--c-surface));
  color: rgb(var(--c-ink));
}
.icon-btn {
  display: grid;
  place-items: center;
  height: 1.75rem;
  width: 1.75rem;
  border-radius: 0.5rem;
}
.icon-btn:hover {
  background: rgb(var(--c-surface));
  color: rgb(var(--c-ink));
}
</style>
