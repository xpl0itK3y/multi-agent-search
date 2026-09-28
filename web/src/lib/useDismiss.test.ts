// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import { effectScope, ref } from "vue";

import { confirmState } from "./confirm";
import { useDismiss } from "./useDismiss";

let scopes: ReturnType<typeof effectScope>[] = [];

function setup() {
  document.body.innerHTML = `
    <button id="trigger">menu</button>
    <div id="menu"><button id="item">item</button></div>
    <p id="outside">page</p>`;
  const trigger = ref(document.getElementById("trigger") as HTMLElement);
  const root = ref(document.getElementById("menu") as HTMLElement);
  const open = ref(true);
  const scope = effectScope();
  scopes.push(scope);
  scope.run(() => useDismiss(root, open, { trigger }));
  return { open, scope, trigger: trigger.value };
}

// Only this composable's document listeners (capture phase, its two event types).
const ownCalls = (spy: { mock: { calls: unknown[][] } }) =>
  spy.mock.calls.filter((c) => (c[0] === "pointerdown" || c[0] === "keydown") && c[2] === true).map((c) => c[0]);

const press = (id: string) =>
  document.getElementById(id)!.dispatchEvent(new Event("pointerdown", { bubbles: true, cancelable: true }));
const key = (k: string, target: EventTarget = document.body) => {
  const e = new KeyboardEvent("keydown", { key: k, bubbles: true, cancelable: true });
  target.dispatchEvent(e);
  return e;
};

describe("useDismiss", () => {
  afterEach(() => {
    for (const scope of scopes) scope.stop();
    scopes = [];
    vi.restoreAllMocks();
    confirmState.open = false;
    document.body.innerHTML = "";
  });

  it("closes on a press outside", () => {
    const { open } = setup();
    press("outside");
    expect(open.value).toBe(false);
  });

  it("stays open for presses inside the popover and on its trigger", () => {
    const { open } = setup();
    press("item");
    press("trigger");
    expect(open.value).toBe(true);
  });

  it("closes on Escape and returns focus to the trigger", () => {
    const { open, trigger } = setup();
    document.getElementById("item")!.focus();
    const e = key("Escape", document.getElementById("item")!);
    expect(open.value).toBe(false);
    expect(document.activeElement).toBe(trigger);
    expect(e.defaultPrevented).toBe(true); // a surrounding sheet stays open
  });

  it("ignores other keys and an Escape already handled", () => {
    const { open } = setup();
    key("Enter");
    const handled = new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true });
    handled.preventDefault();
    document.body.dispatchEvent(handled);
    expect(open.value).toBe(true);
  });

  it("stays open while a confirmation it opened is up", () => {
    const { open } = setup();
    confirmState.open = true;
    press("outside");
    key("Escape");
    expect(open.value).toBe(true);
  });

  it("listens only while open", () => {
    const remove = vi.spyOn(document, "removeEventListener");
    const add = vi.spyOn(document, "addEventListener");
    const { open, scope } = setup();
    expect(ownCalls(add).sort()).toEqual(["keydown", "pointerdown"]);

    open.value = false;
    expect(ownCalls(remove).sort()).toEqual(["keydown", "pointerdown"]);

    press("outside");
    open.value = true;
    expect(ownCalls(add)).toHaveLength(4);
    press("item");
    expect(open.value).toBe(true);

    scope.stop();
    expect(ownCalls(remove)).toHaveLength(4);
    press("outside");
    expect(open.value).toBe(true);
  });
});
