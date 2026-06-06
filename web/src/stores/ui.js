import { defineStore } from "pinia";
import { ref } from "vue";
export const useUiStore = defineStore("ui", () => {
    const sidebarCollapsed = ref(false);
    const userName = ref(import.meta.env.VITE_USER_NAME || "denis");
    function toggleSidebar() {
        sidebarCollapsed.value = !sidebarCollapsed.value;
    }
    return { sidebarCollapsed, userName, toggleSidebar };
});
