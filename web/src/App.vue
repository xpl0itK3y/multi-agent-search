<script setup lang="ts">
import { onMounted, watch } from "vue";
import { useRoute } from "vue-router";
import AppSidebar from "@/components/AppSidebar.vue";
import ConfirmDialog from "@/components/ConfirmDialog.vue";
import { useResearchStore } from "@/stores/research";
import { useUiStore } from "@/stores/ui";

const store = useResearchStore();
const ui = useUiStore();
const route = useRoute();

onMounted(() => {
  store.fetchModels();
  store.fetchHistory();
});

// Close the mobile drawer on any navigation.
watch(() => route.fullPath, () => ui.closeMobile());
</script>

<template>
  <!-- Site-styled confirm modal (replaces window.confirm), available app-wide -->
  <ConfirmDialog />

  <!-- Login screen: no app shell -->
  <div v-if="route.name === 'login'" class="h-screen w-screen overflow-hidden bg-bg text-ink">
    <router-view />
  </div>

  <div v-else class="flex h-screen w-screen overflow-hidden bg-bg text-ink">
    <!-- Sidebar: static on lg+, off-canvas drawer on mobile -->
    <div
      class="fixed inset-y-0 left-0 z-40 transform transition-transform duration-200 lg:static lg:z-auto lg:translate-x-0"
      :class="ui.mobileOpen ? 'translate-x-0' : '-translate-x-full'"
    >
      <AppSidebar />
    </div>

    <!-- Mobile backdrop -->
    <div
      v-if="ui.mobileOpen"
      class="fixed inset-0 z-30 bg-black/50 lg:hidden"
      @click="ui.closeMobile()"
    />

    <main class="relative min-h-0 flex-1 overflow-hidden">
      <!-- Mobile menu button (top-right avoids the views' top-left back button) -->
      <button
        class="absolute right-3 top-3 z-20 grid h-9 w-9 place-items-center rounded-lg border border-bd bg-rail text-ink lg:hidden"
        aria-label="Menu"
        @click="ui.toggleMobile()"
      >
        ☰
      </button>
      <router-view v-slot="{ Component }">
        <transition name="view" mode="out-in">
          <component :is="Component" :key="route.fullPath" />
        </transition>
      </router-view>
    </main>
  </div>
</template>
