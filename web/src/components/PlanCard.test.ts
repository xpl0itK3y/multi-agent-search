// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import { mount, type VueWrapper } from "@vue/test-utils";

import { i18n } from "@/i18n";
import type { PlanItem } from "@/lib/types";
import PlanCard from "./PlanCard.vue";

const t = (key: string) => i18n.global.t(key);

function mountPlan(items: PlanItem[]) {
  return mount(PlanCard, { props: { prompt: "Topic", items }, global: { plugins: [i18n] } });
}

const buttonNamed = (wrapper: VueWrapper, text: string) => wrapper.findAll("button").find((b) => b.text() === text)!;
const approved = (wrapper: VueWrapper) => wrapper.emitted("approve")![0][0] as PlanItem[];

const plan: PlanItem[] = [
  { id: "a", description: "First", queries: ["a one", "a two"] },
  { id: "b", description: "Second", queries: ["b one"] },
  { id: "c", description: "Third", queries: ["c one"] },
];

describe("PlanCard editing", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("puts a removed row back at its place, with its queries", async () => {
    const wrapper = mountPlan(plan);

    await wrapper.findAll(`button[aria-label="${t("plan.delete")}"]`)[1].trigger("click");
    expect(wrapper.text()).toContain(t("plan.removed"));
    expect(wrapper.text()).not.toContain("Second");

    await buttonNamed(wrapper, t("plan.undo")).trigger("click");
    expect(wrapper.text()).not.toContain(t("plan.removed"));

    await buttonNamed(wrapper, t("plan.run")).trigger("click");
    expect(approved(wrapper)).toEqual(plan);
  });

  it("lets the undo go after a while, or on the next edit", async () => {
    vi.useFakeTimers();
    const wrapper = mountPlan(plan);

    await wrapper.findAll(`button[aria-label="${t("plan.delete")}"]`)[0].trigger("click");
    vi.advanceTimersByTime(6000);
    await wrapper.vm.$nextTick();
    expect(wrapper.text()).not.toContain(t("plan.removed"));

    await wrapper.findAll(`button[aria-label="${t("plan.delete")}"]`)[0].trigger("click");
    expect(wrapper.text()).toContain(t("plan.removed"));
    await wrapper.find("input").setValue("Edited");
    expect(wrapper.text()).not.toContain(t("plan.removed"));
  });

  it("searches a sub-question typed without queries as written", async () => {
    const wrapper = mountPlan([{ id: "a", description: "  Only a question  ", queries: [] }]);

    await buttonNamed(wrapper, t("plan.run")).trigger("click");

    expect(approved(wrapper)).toEqual([{ id: "a", description: "Only a question", queries: ["Only a question"] }]);
  });

  it("drops only rows left completely empty", async () => {
    const wrapper = mountPlan([plan[0]]);

    await buttonNamed(wrapper, t("plan.add")).trigger("click");
    await buttonNamed(wrapper, t("plan.run")).trigger("click");

    expect(approved(wrapper)).toEqual([plan[0]]);
  });
});
