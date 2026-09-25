// @vitest-environment jsdom
import { beforeEach, describe, expect, it } from "vitest";
import { mount } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createMemoryHistory, createRouter } from "vue-router";

import { i18n } from "@/i18n";
import { useAuthStore } from "@/stores/auth";
import { useUiStore } from "@/stores/ui";
import AppSidebar from "./AppSidebar.vue";

const router = createRouter({
  history: createMemoryHistory(),
  routes: [{ path: "/", component: { template: "<div />" } }],
});

function mountWithAvatar(avatarUrl: string | null, collapsed: boolean) {
  const pinia = createPinia();
  setActivePinia(pinia);
  useAuthStore().user = { id: "u1", email: "denis@example.com", name: "Denis", avatar_url: avatarUrl } as never;
  useUiStore().sidebarCollapsed = collapsed;
  return mount(AppSidebar, { global: { plugins: [pinia, router, i18n] } });
}

describe("AppSidebar avatar", () => {
  beforeEach(async () => {
    await router.push("/");
  });

  for (const collapsed of [false, true]) {
    it(`renders an emoji preset as text, not as <img src> (collapsed=${collapsed})`, () => {
      const wrapper = mountWithAvatar("🤖", collapsed);

      expect(wrapper.find('img[src="🤖"]').exists()).toBe(false);
      expect(wrapper.text()).toContain("🤖");
    });

    it(`keeps rendering a real photo URL as an image (collapsed=${collapsed})`, () => {
      const wrapper = mountWithAvatar("https://lh3.googleusercontent.com/a/photo", collapsed);

      expect(wrapper.find('img[src="https://lh3.googleusercontent.com/a/photo"]').exists()).toBe(true);
    });
  }
});

describe("AppSidebar logout", () => {
  beforeEach(async () => {
    await router.push("/");
  });

  it("names the logout button for what it does: every device is signed out", () => {
    i18n.global.locale.value = "en";
    const wrapper = mountWithAvatar(null, false);

    const button = wrapper.find(`button[aria-label="${i18n.global.t("auth.logoutEverywhere")}"]`);
    expect(button.exists()).toBe(true);
    expect(button.attributes("title")).toBe("Log out on all devices");
  });
});
