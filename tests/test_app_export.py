from src.agents.app_export import AppExportAgent
from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth
from src.repositories.in_memory_task_store import InMemoryTaskStore
from src.services.research_service import ResearchService


class _LLM:
    def __init__(self, out):
        self.out = out
        self.models = []

    def generate(self, system_prompt, user_prompt, **kwargs):
        self.models.append(kwargs.get("model"))
        return self.out


_DOC = "<!DOCTYPE html><html><body>X</body></html>"


def test_app_export_returns_html():
    assert AppExportAgent(_LLM(_DOC)).generate("make a landing", "topic", "## Report\nBody [S1].") == _DOC


def test_app_export_strips_markdown_fences():
    out = AppExportAgent(_LLM(f"```html\n{_DOC}\n```")).generate("brief", "t", "report")
    assert out.startswith("<!DOCTYPE html>") and "```" not in out and "<body>X</body>" in out


def test_app_export_rejects_non_html():
    assert AppExportAgent(_LLM("Sorry, I cannot help with that.")).generate("brief", "t", "report") == ""


def test_app_export_no_llm_or_empty_report():
    assert AppExportAgent(None).generate("b", "t", "r") == ""
    assert AppExportAgent(_LLM(_DOC)).generate("b", "t", "   ") == ""


def test_generate_app_export_via_service():
    store = InMemoryTaskStore()
    svc = ResearchService(task_store=store, app_export_agent=AppExportAgent(_LLM(_DOC)))
    rec = store.add_research(ResearchRequest(prompt="Topic here", depth=SearchDepth.EASY), task_ids=[])
    store.update_research_status(rec.id, ResearchStatus.COMPLETED, "## Report\nBody.")
    data, media, name = svc.generate_app_export(rec.id, "make a one-pager for investors")
    assert media.startswith("text/html") and name.endswith(".html")
    assert b"<body>X</body>" in data
