// @vitest-environment jsdom
import { beforeEach, describe, expect, it } from "vitest";
import { createPinia, setActivePinia } from "pinia";

import { i18n } from "@/i18n";
import { useUiStore } from "./ui";

describe("ui store locale", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    i18n.global.locale.value = "es";
    document.documentElement.lang = "ru"; // what index.html ships
  });

  it("sets <html lang> from the active locale at startup and on every change", () => {
    const ui = useUiStore();
    expect(document.documentElement.lang).toBe("es");

    ui.setLocale("en");
    expect(document.documentElement.lang).toBe("en");
    expect(i18n.global.locale.value).toBe("en");
  });
});
