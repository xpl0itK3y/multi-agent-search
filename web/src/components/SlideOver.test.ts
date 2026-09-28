// @vitest-environment jsdom
import { afterEach, describe, expect, it } from "vitest";
import { mount, type VueWrapper } from "@vue/test-utils";
import { nextTick } from "vue";

import SlideOver from "./SlideOver.vue";

let wrapper: VueWrapper | null = null;

function mountSheet(props: Record<string, unknown> = {}) {
  wrapper = mount(SlideOver, {
    props: { open: false, label: "Details", ...props },
    slots: { default: '<button id="first">First</button><input id="field" /><button id="last">Last</button>' },
    attachTo: document.body,
  });
  return wrapper;
}

const panel = () => wrapper!.find('[data-test="slideover-panel"]');
const scrim = () => wrapper!.find('[data-test="slideover-scrim"]');
const fire = (el: Element, type: string) => el.dispatchEvent(new Event(type, { bubbles: true, cancelable: true }));
const key = (k: string, opts: KeyboardEventInit = {}) => {
  const e = new KeyboardEvent("keydown", { key: k, bubbles: true, cancelable: true, ...opts });
  (document.activeElement ?? document.body).dispatchEvent(e);
  return e;
};

describe("SlideOver", () => {
  afterEach(() => {
    wrapper?.unmount();
    wrapper = null;
    document.body.innerHTML = "";
  });

  it("is inert and hidden from assistive tech while closed", () => {
    mountSheet();
    expect(panel().attributes("inert")).toBeDefined();
    expect(panel().attributes("aria-hidden")).toBe("true");
    expect(panel().attributes("role")).toBe("dialog");
    expect(panel().attributes("aria-label")).toBe("Details");
  });

  it("moves focus inside on open and gives it back on close", async () => {
    const opener = document.createElement("button");
    document.body.appendChild(opener);
    opener.focus();
    mountSheet();

    await wrapper!.setProps({ open: true });
    await nextTick();
    expect(panel().attributes("inert")).toBeUndefined();
    expect(panel().attributes("aria-modal")).toBe("true");
    expect(document.activeElement?.id).toBe("first");

    await wrapper!.setProps({ open: false });
    await nextTick();
    expect(document.activeElement).toBe(opener);
  });

  it("focuses [data-autofocus] when there is one", async () => {
    wrapper = mount(SlideOver, {
      props: { open: false },
      slots: { default: '<button>One</button><input id="auto" data-autofocus />' },
      attachTo: document.body,
    });
    await wrapper.setProps({ open: true });
    await nextTick();
    expect(document.activeElement?.id).toBe("auto");
  });

  it("closes on Escape", async () => {
    mountSheet({ open: true });
    await nextTick();
    key("Escape");
    expect(wrapper!.emitted("close")).toHaveLength(1);
  });

  it("leaves an Escape that an inner popover already handled", async () => {
    mountSheet({ open: true });
    await nextTick();
    const e = new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true });
    e.preventDefault();
    document.body.dispatchEvent(e);
    expect(wrapper!.emitted("close")).toBeUndefined();
  });

  it("keeps Tab inside a modal sheet", async () => {
    mountSheet({ open: true });
    await nextTick();
    (document.getElementById("last") as HTMLElement).focus();
    expect(key("Tab").defaultPrevented).toBe(true);
    expect(document.activeElement?.id).toBe("first");

    expect(key("Tab", { shiftKey: true }).defaultPrevented).toBe(true);
    expect(document.activeElement?.id).toBe("last");
  });

  it("does not close when a press starts in the panel and ends on the scrim", async () => {
    mountSheet({ open: true });
    await nextTick();
    fire(panel().element, "pointerdown");
    fire(scrim().element, "click");
    expect(wrapper!.emitted("close")).toBeUndefined();
  });

  it("closes when the press starts and ends on the scrim", async () => {
    mountSheet({ open: true });
    await nextTick();
    fire(scrim().element, "pointerdown");
    fire(scrim().element, "click");
    expect(wrapper!.emitted("close")).toHaveLength(1);
  });

  it("renders no scrim and no focus trap when non-modal", async () => {
    mountSheet({ open: true, modal: false });
    await nextTick();
    expect(scrim().exists()).toBe(false);
    expect(panel().attributes("aria-modal")).toBeUndefined();
    (document.getElementById("last") as HTMLElement).focus();
    expect(key("Tab").defaultPrevented).toBe(false);
  });

  it("slides from its side and back the same way", async () => {
    mountSheet({ side: "left" });
    expect(panel().classes()).toContain("-translate-x-full");
    expect(panel().classes()).toContain("left-0");
    await wrapper!.setProps({ open: true });
    expect(panel().classes()).toContain("translate-x-0");
    await wrapper!.setProps({ open: false, side: "right" });
    expect(panel().classes()).toContain("translate-x-full");
  });
});
