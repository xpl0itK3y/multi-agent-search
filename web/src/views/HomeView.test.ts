// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount, type VueWrapper } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createMemoryHistory, createRouter } from "vue-router";

import { i18n } from "@/i18n";
import HomeView from "./HomeView.vue";

const t = (key: string) => i18n.global.t(key);

let wrapper: VueWrapper | null = null;

async function mountHome() {
  const pinia = createPinia();
  setActivePinia(pinia);
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/", component: HomeView },
      { path: "/thread/:threadId", name: "thread", component: { render: () => null } },
    ],
  });
  await router.push("/");
  wrapper = mount(HomeView, { attachTo: document.body, global: { plugins: [pinia, router, i18n] } });
  await flushPromises();
  return wrapper;
}

const textarea = () => wrapper!.find("textarea");
const value = () => (textarea().element as HTMLTextAreaElement).value;
const chip = (key: string) => wrapper!.findAll("button").find((b) => b.text().includes(t(`chips.${key}.label`)))!;
const example = (id: string) => wrapper!.findAll("button").find((b) => b.text().includes(t(`home.examples.${id}.title`)))!;
const restore = () => wrapper!.findAll("button").find((b) => b.text() === t("home.restorePrompt"));

describe("HomeView prompt helpers", () => {
  beforeEach(() => {
    i18n.global.locale.value = "en";
  });
  afterEach(() => {
    wrapper?.unmount();
    wrapper = null;
    vi.useRealTimers();
  });

  it("a chip fills an empty prompt with its template", async () => {
    await mountHome();
    await chip("compare").trigger("click");
    expect(value()).toBe(t("chips.compare.template"));
  });

  it("a chip goes in front of what was typed instead of wiping it", async () => {
    await mountHome();
    await textarea().setValue("  Postgres vs SQLite for a CLI  ");
    await chip("compare").trigger("click");
    expect(value()).toBe(t("chips.compare.template") + "Postgres vs SQLite for a CLI");
  });

  it("another chip swaps only the prefix", async () => {
    await mountHome();
    await textarea().setValue("Postgres vs SQLite");
    await chip("compare").trigger("click");
    await chip("market").trigger("click");
    expect(value()).toBe(t("chips.market.template") + "Postgres vs SQLite");
  });

  it("an example replaces typed text and offers it back", async () => {
    await mountHome();
    await textarea().setValue("my own question");
    await example("rag").trigger("click");
    await flushPromises();

    expect(value()).toBe(t("home.examples.rag.prompt"));
    await restore()!.trigger("click");
    await flushPromises();
    expect(value()).toBe("my own question");
    expect(restore()).toBeUndefined();
  });

  it("offers nothing back when there was nothing typed", async () => {
    await mountHome();
    await example("rag").trigger("click");
    await flushPromises();
    expect(value()).toBe(t("home.examples.rag.prompt"));
    expect(restore()).toBeUndefined();
  });

  it("a second example still restores the text typed before the first", async () => {
    await mountHome();
    await textarea().setValue("my own question");
    await example("rag").trigger("click");
    await flushPromises();
    await example("ai").trigger("click");
    await flushPromises();

    await restore()!.trigger("click");
    await flushPromises();
    expect(value()).toBe("my own question");
  });

  it("drops the offer on the next edit and after 8 seconds", async () => {
    vi.useFakeTimers();
    await mountHome();
    await textarea().setValue("my own question");
    await example("rag").trigger("click");
    await flushPromises();
    await textarea().setValue(t("home.examples.rag.prompt") + " and more");
    await flushPromises();
    expect(restore()).toBeUndefined();

    await textarea().setValue("again");
    await example("ai").trigger("click");
    await flushPromises();
    expect(restore()).toBeDefined();
    vi.advanceTimersByTime(8000);
    await flushPromises();
    expect(restore()).toBeUndefined();
  });
});
