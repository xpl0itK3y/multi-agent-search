// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount, type VueWrapper } from "@vue/test-utils";

import { answerConfirm, confirm, confirmState } from "@/lib/confirm";
import ConfirmDialog from "./ConfirmDialog.vue";

let wrapper: VueWrapper | null = null;
let trigger: HTMLButtonElement | null = null;

async function open(danger: boolean) {
  trigger = document.createElement("button");
  trigger.textContent = "✕";
  document.body.appendChild(trigger);
  trigger.focus();
  wrapper = mount(ConfirmDialog, { attachTo: document.body });
  const answer = confirm({ title: "Delete", message: "Gone for good?", confirmText: "Delete", cancelText: "Cancel", danger });
  await flushPromises();
  // Wrapped: an async function returning the promise itself would wait for the answer.
  return { answer };
}

const button = (label: string) => wrapper!.findAll("button").find((b) => b.text() === label)!;
const key = (k: string, init: KeyboardEventInit = {}) =>
  (document.activeElement ?? document.body).dispatchEvent(new KeyboardEvent("keydown", { key: k, bubbles: true, cancelable: true, ...init }));

afterEach(() => {
  answerConfirm(false);
  wrapper?.unmount();
  wrapper = null;
  trigger?.remove();
  trigger = null;
  vi.useRealTimers();
});

describe("ConfirmDialog", () => {
  it("is an alertdialog named by its title and described by its message", async () => {
    await open(true);
    const panel = wrapper!.find("[role='alertdialog']");

    expect(panel.attributes("aria-modal")).toBe("true");
    expect(document.getElementById(panel.attributes("aria-labelledby")!)!.textContent).toBe("Delete");
    expect(document.getElementById(panel.attributes("aria-describedby")!)!.textContent).toBe("Gone for good?");
  });

  it("focuses Cancel for a destructive action", async () => {
    await open(true);
    expect(document.activeElement).toBe(button("Cancel").element);
  });

  it("focuses Confirm for a harmless action", async () => {
    await open(false);
    expect(document.activeElement).toBe(button("Delete").element);
  });

  it("never confirms a destructive action on a bare Enter: it answers for the focused button", async () => {
    const { answer } = await open(true);
    key("Enter");
    expect(await answer).toBe(false);
  });

  it("Enter on the focused destructive button (after Tab) confirms", async () => {
    const { answer } = await open(true);
    key("Tab");
    expect(document.activeElement).toBe(button("Delete").element);
    key("Enter");
    expect(await answer).toBe(true);
  });

  it("Enter confirms a harmless action", async () => {
    const { answer } = await open(false);
    (document.activeElement as HTMLElement).blur();
    key("Enter");
    expect(await answer).toBe(true);
  });

  it("does not confirm a destructive action on Enter when no button has focus", async () => {
    await open(true);
    (document.activeElement as HTMLElement).blur();
    key("Enter");
    expect(confirmState.open).toBe(true);
  });

  it("Tab and Shift+Tab cycle between the two buttons", async () => {
    await open(true);
    key("Tab");
    expect(document.activeElement).toBe(button("Delete").element);
    key("Tab");
    expect(document.activeElement).toBe(button("Cancel").element);
    key("Tab", { shiftKey: true });
    expect(document.activeElement).toBe(button("Delete").element);
  });

  it("Escape resolves false", async () => {
    const { answer } = await open(false);
    key("Escape");
    expect(await answer).toBe(false);
  });

  it("a press that starts on the panel and ends on the backdrop does not close", async () => {
    await open(true);
    await wrapper!.find("[data-test='confirm-panel']").trigger("pointerdown");
    await wrapper!.find("[data-test='confirm-backdrop']").trigger("click");
    expect(confirmState.open).toBe(true);
  });

  it("a plain click on the backdrop cancels", async () => {
    const { answer } = await open(true);
    const backdrop = wrapper!.find("[data-test='confirm-backdrop']");
    await backdrop.trigger("pointerdown");
    await backdrop.trigger("click");
    expect(await answer).toBe(false);
  });

  it("gives focus back to the element that had it", async () => {
    vi.useFakeTimers();
    const { answer } = await open(true);
    await button("Cancel").trigger("click");
    await answer;
    vi.runAllTimers();
    expect(document.activeElement).toBe(trigger);
  });
});
