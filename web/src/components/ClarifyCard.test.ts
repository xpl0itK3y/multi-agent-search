// @vitest-environment jsdom
import { afterEach, describe, expect, it } from "vitest";
import { mount } from "@vue/test-utils";

import { i18n } from "@/i18n";
import ClarifyCard from "./ClarifyCard.vue";

describe("ClarifyCard", () => {
  afterEach(() => {
    document.body.innerHTML = "";
  });

  it("moves to the next answer on Enter and sends only from the last one", async () => {
    const wrapper = mount(ClarifyCard, {
      props: { prompt: "Topic", questions: ["Which region?", "Which years?"] },
      global: { plugins: [i18n] },
      attachTo: document.body,
    });
    const [first, second] = wrapper.findAll("input");

    await first.setValue("Europe");
    await first.trigger("keydown", { key: "Enter" });
    expect(wrapper.emitted("submit")).toBeUndefined();
    expect(document.activeElement).toBe(second.element);

    await second.setValue("2020–2026");
    await second.trigger("keydown", { key: "Enter" });
    expect(wrapper.emitted("submit")).toEqual([[["Europe", "2020–2026"]]]);
    wrapper.unmount();
  });

  it("labels each answer with its question", () => {
    const wrapper = mount(ClarifyCard, {
      props: { prompt: "Topic", questions: ["Which region?"] },
      global: { plugins: [i18n] },
    });
    const input = wrapper.get("input");
    expect(wrapper.get(`label[for="${input.attributes("id")}"]`).text()).toBe("Which region?");
  });
});
