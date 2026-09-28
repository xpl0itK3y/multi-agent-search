// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount, type VueWrapper } from "@vue/test-utils";

import { i18n } from "@/i18n";

const mocks = vi.hoisted(() => ({
  api: {
    getShare: vi.fn(),
    createShare: vi.fn(),
    revokeShare: vi.fn(),
    getComparison: vi.fn(),
    getCitations: vi.fn(),
    getSourceIndependence: vi.fn(),
    getSourceReputation: vi.fn(),
    getStance: vi.fn(),
    getSourceIntegrity: vi.fn(),
    getCrossLanguage: vi.fn(),
    getConfidence: vi.fn(),
    getNumericCheck: vi.fn(),
    getSources: vi.fn(),
    getConflicts: vi.fn(),
    getVerification: vi.fn(),
    getRedTeam: vi.fn(),
    getGraph: vi.fn(),
    exportReport: vi.fn(),
  },
  confirm: vi.fn(),
}));

vi.mock("@/lib/api", () => ({ api: mocks.api, apiErrorMessage: () => "request failed" }));
// Keep the real confirmState (useDismiss reads it); only the dialog answer is scripted.
vi.mock("@/lib/confirm", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@/lib/confirm")>()),
  confirm: mocks.confirm,
}));

import ArtifactPanel from "./ArtifactPanel.vue";

const t = (key: string) => i18n.global.t(key) as string;

// Every optional trust fetch fails quietly unless a test says otherwise.
function quietOptionalFetches() {
  for (const fn of [
    mocks.api.getComparison,
    mocks.api.getCitations,
    mocks.api.getSourceIndependence,
    mocks.api.getSourceReputation,
    mocks.api.getStance,
    mocks.api.getSourceIntegrity,
    mocks.api.getCrossLanguage,
    mocks.api.getConfidence,
    mocks.api.getNumericCheck,
  ]) {
    fn.mockRejectedValue(new Error("404"));
  }
}

let wrapper: VueWrapper | null = null;

async function mountPanel(props: { isFinal?: boolean; report?: string } = {}) {
  wrapper = mount(ArtifactPanel, {
    props: { id: "r-1", report: props.report ?? "# Report\n\nBody [S1].", isFinal: props.isFinal ?? true },
    global: {
      plugins: [i18n],
      stubs: { MarkdownView: true, ResearchDashboard: true, ReportSkeletonCanvas: true, SourceCard: true },
    },
    attachTo: document.body,
  });
  await flushPromises();
  return wrapper;
}

function buttonWithText(w: VueWrapper, text: string) {
  const found = w.findAll("button").find((b) => b.text().includes(text));
  if (!found) throw new Error(`no button with text ${text}`);
  return found;
}

beforeEach(() => {
  quietOptionalFetches();
  mocks.api.getShare.mockResolvedValue({ shared: false, token: "" });
});

afterEach(() => {
  wrapper?.unmount();
  wrapper = null;
  vi.clearAllMocks();
  vi.useRealTimers();
});

describe("ArtifactPanel sharing", () => {
  it("loads the share state on a final report without an unhandled rejection", async () => {
    const rejections: unknown[] = [];
    const onRejection = (reason: unknown) => rejections.push(reason);
    process.on("unhandledRejection", onRejection);
    try {
      mocks.api.getShare.mockResolvedValue({ shared: true, token: "tok" });
      const w = await mountPanel();
      await new Promise((r) => setTimeout(r, 0));

      expect(rejections).toEqual([]);
      expect(mocks.api.getShare).toHaveBeenCalledWith("r-1");
      expect(mocks.api.getComparison).toHaveBeenCalledWith("r-1");
      // An already shared report says so on its button.
      expect(w.text()).toContain(t("share.shared"));
    } finally {
      process.off("unhandledRejection", onRejection);
    }
  });

  it("opening the popover publishes nothing; Create does, once", async () => {
    mocks.api.createShare.mockResolvedValue({ shared: true, token: "tok" });
    const w = await mountPanel();

    await buttonWithText(w, t("share.share")).trigger("click");
    await flushPromises();
    expect(mocks.api.createShare).not.toHaveBeenCalled();
    expect(w.find("input[readonly]").exists()).toBe(false);

    await buttonWithText(w, t("share.create")).trigger("click");
    await flushPromises();
    expect(mocks.api.createShare).toHaveBeenCalledTimes(1);
    expect((w.find("input[readonly]").element as HTMLInputElement).value).toContain("/r/tok");
  });

  it("shows a failed Create inside the popover", async () => {
    mocks.api.createShare.mockRejectedValue(new Error("offline"));
    const w = await mountPanel();

    await buttonWithText(w, t("share.share")).trigger("click");
    await buttonWithText(w, t("share.create")).trigger("click");
    await flushPromises();

    expect(w.find("[role=alert]").text()).toBe("request failed");
  });

  it("revokes only after the confirmation, and keeps the popover open", async () => {
    mocks.api.getShare.mockResolvedValue({ shared: true, token: "tok" });
    mocks.api.revokeShare.mockResolvedValue({ shared: false, token: "" });
    const w = await mountPanel();
    await buttonWithText(w, t("share.shared")).trigger("click");

    mocks.confirm.mockResolvedValueOnce(false);
    await buttonWithText(w, t("share.revoke")).trigger("click");
    await flushPromises();
    expect(mocks.confirm).toHaveBeenCalledTimes(1);
    expect(mocks.confirm.mock.calls[0][0]).toMatchObject({ danger: true, message: t("share.revokeConfirm") });
    expect(mocks.api.revokeShare).not.toHaveBeenCalled();

    mocks.confirm.mockResolvedValueOnce(true);
    await buttonWithText(w, t("share.revoke")).trigger("click");
    await flushPromises();
    expect(mocks.api.revokeShare).toHaveBeenCalledTimes(1);
    expect(w.find("[role=status]").text()).toBe(t("share.revoked"));
    expect(buttonWithText(w, t("share.create")).exists()).toBe(true);
  });
});
