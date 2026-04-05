import json

import pytest
from fastapi import HTTPException

from src.agents.analyzer import AnalyzerAgent
from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth, SearchTask, TaskStatus, SearchJobStatus
from src.api.schemas import ExtractionMetrics
from src.core.llm import LLMProvider
from src.repositories import InMemoryTaskStore
from src.services.research_service import ResearchService
from src.search_depth_profiles import get_depth_profile


class RecordingLLM(LLMProvider):
    def __init__(self, response: str = "ok"):
        self.response = response
        self.calls = []

    def generate(self, system_prompt: str, user_prompt: str, **kwargs) -> str:
        self.calls.append(
            {
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "kwargs": kwargs,
            }
        )
        return self.response


class SequentialLLM(LLMProvider):
    def __init__(self, responses: list[str]):
        self.responses = list(responses)
        self.calls = []

    def generate(self, system_prompt: str, user_prompt: str, **kwargs) -> str:
        self.calls.append(
            {
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "kwargs": kwargs,
            }
        )
        return self.responses.pop(0)


def test_decompose_requires_initialized_orchestrator(mocker):
    service = ResearchService(task_store=InMemoryTaskStore(), orchestrator=None)

    with pytest.raises(HTTPException) as exc_info:
        service.decompose_prompt(
            "test",
            SearchDepth.EASY,
        )

    assert exc_info.value.status_code == 503


def test_analyzer_agent_uses_llm_provider_contract():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[{"url": "https://example.com", "title": "Example", "content": "Body"}],
            )
        ],
    )

    assert result.endswith("## Sources")
    assert "## Report Notes" in result
    assert len(llm.calls) == 1
    assert llm.calls[0]["system_prompt"] == agent.SYSTEM_PROMPT
    assert "original prompt" in llm.calls[0]["user_prompt"]
    assert llm.calls[0]["kwargs"]["temperature"] == 0.3
    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    assert parsed["gathered_data"][0]["source_id"] == "S1"
    assert parsed["gathered_data"][0]["domain"] == "example.com"
    assert parsed["gathered_data"][0]["source_quality"] == "low"
    assert "Use inline source references like [S1], [S2]" in agent.SYSTEM_PROMPT
    assert "Prefer higher-quality and more authoritative sources when sources conflict" in agent.SYSTEM_PROMPT


def test_analyzer_agent_repairs_structured_reports_with_uncited_claims():
    llm = SequentialLLM(
        [
            "## Introduction\nThis paragraph makes a factual claim without citations.\n\n## Conclusion\nAnother factual claim without citations.\n\n## Sources",
            "## Introduction\nThis paragraph makes a factual claim with support [S1].\n\n## Conclusion\nAnother factual claim with support [S1].\n\n## Sources\n- [S1] https://example.com",
        ]
    )
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[{"url": "https://example.com", "title": "Example", "content": "Body " * 100}],
            )
        ],
    )

    assert len(llm.calls) == 2
    assert "[S1]" in result
    assert "## Sources" in result


def test_analyzer_agent_repairs_weakly_supported_citations():
    llm = SequentialLLM(
        [
            "## Introduction\nPython packaging metadata and wheel tags are central here [S1].\n\n## Conclusion\nDone [S1].\n\n## Sources\n- [S1] https://example.com/a",
            "## Introduction\nAsyncIO event loop behavior is central here [S1].\n\n## Conclusion\nDone [S1].\n\n## Sources\n- [S1] https://example.com/a",
        ]
    )
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://example.com/a",
                        "title": "AsyncIO",
                        "content": "AsyncIO event loop scheduling and cooperative concurrency are central for Python network services. "
                        * 20,
                    }
                ],
            )
        ],
    )

    assert len(llm.calls) == 2
    assert "Likely weakly-supported cited lines" in llm.calls[1]["user_prompt"]
    assert "AsyncIO event loop behavior is central here [S1]." in result


def test_analyzer_agent_limits_duplicate_domains_during_source_selection():
    llm = RecordingLLM(response="## Introduction\nSummary [S1].\n\n## Conclusion\nDone [S2].\n\n## Sources")
    agent = AnalyzerAgent(llm)

    agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": f"https://example.com/{index}",
                        "domain": "example.com",
                        "source_quality": "medium",
                        "title": f"Title {index}",
                        "content": f"Useful detailed body {index} " * 50,
                    }
                    for index in range(5)
                ]
                + [
                    {
                        "url": "https://docs.python.org/3/tutorial/",
                        "domain": "docs.python.org",
                        "source_quality": "high",
                        "title": "Python Tutorial",
                        "content": "Useful Python tutorial body " * 80,
                    }
                ],
            )
        ],
    )

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    gathered = parsed["gathered_data"]
    example_sources = [item for item in gathered if item["domain"] == "example.com"]
    assert len(example_sources) <= 2


def test_analyzer_agent_filters_failed_and_duplicate_sources():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {"url": "https://example.com/a", "title": "Example", "content": "Useful body " * 40},
                    {"url": "https://example.com/b", "title": "Example", "content": "Useful body " * 40},
                    {"url": "https://example.com/c", "title": "Bad", "content": "Failed to extract content"},
                ],
            )
        ],
    )

    assert result.endswith("## Sources")
    assert "## Report Notes" in result
    assert len(llm.calls) == 1
    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    gathered = parsed["gathered_data"]
    assert len(gathered) == 1
    assert gathered[0]["source_id"] == "S1"
    assert gathered[0]["url"] == "https://example.com/a"


def test_analyzer_agent_limits_prepared_source_count():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    tasks = [
        SearchTask(
            id="task-1",
            description="desc",
            queries=["query"],
            status=TaskStatus.COMPLETED,
            result=[
                {
                    "url": f"https://example.com/{index}",
                    "title": f"Title {index}",
                    "content": f"Body {index} " * 100,
                }
                for index in range(30)
            ],
        )
    ]

    agent.run_analysis("original prompt", tasks)

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    gathered = parsed["gathered_data"]
    assert len(gathered) == 12
    assert gathered[0]["source_id"] == "S1"
    assert gathered[-1]["source_id"] == "S12"


def test_analyzer_agent_compacts_source_content_before_prompt_payload():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)
    long_content = (
        "Sentence one explains the framework architecture in detail. "
        "Sentence two explains request validation and response modeling clearly. "
        "Sentence three explains operational tradeoffs for production services. "
        "Sentence four explains async support and performance characteristics. "
    ) * 30

    agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://example.com/long",
                        "title": "Long source",
                        "content": long_content,
                    }
                ],
            )
        ],
    )

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    gathered = parsed["gathered_data"]
    assert len(gathered[0]["content"]) <= 1605
    assert gathered[0]["content"].endswith(" ...")


def test_analyzer_agent_prefers_trusted_domains_for_similar_sources():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://generic-blog.example/python",
                        "title": "Python Tutorial",
                        "content": "Useful Python tutorial body " * 80,
                    },
                    {
                        "url": "https://docs.python.org/3/tutorial/",
                        "title": "Python Tutorial",
                        "content": "Useful Python tutorial body " * 80,
                    },
                ],
            )
        ],
    )

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    gathered = parsed["gathered_data"]
    assert len(gathered) == 1
    assert gathered[0]["url"] == "https://docs.python.org/3/tutorial/"
    assert gathered[0]["source_id"] == "S1"


def test_analyzer_agent_prefers_programming_docs_over_generic_tutorial_sites():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    agent.run_analysis(
        "Сравни документацию FastAPI и общие python API guides",
        [
            SearchTask(
                id="task-1",
                description="Документация FastAPI и Python API",
                queries=["fastapi api documentation"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://fastapi.tiangolo.com/",
                        "domain": "fastapi.tiangolo.com",
                        "source_quality": "high",
                        "title": "FastAPI Documentation",
                        "content": "Official documentation and API reference for FastAPI with tutorials and reference guide content. " * 20,
                    },
                    {
                        "url": "https://developer.mozilla.org/en-US/docs/Web/HTTP",
                        "domain": "developer.mozilla.org",
                        "source_quality": "high",
                        "title": "MDN HTTP documentation",
                        "content": "Official HTTP docs and reference guide used by developers in production systems. " * 20,
                    },
                    {
                        "url": "https://www.geeksforgeeks.org/top-python-frameworks/",
                        "domain": "www.geeksforgeeks.org",
                        "source_quality": "medium",
                        "title": "Top Python frameworks",
                        "content": "This comparison blog lists popular frameworks and opinions about which framework to choose. " * 12,
                    },
                ],
            )
        ],
    )

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    urls = [item["url"] for item in parsed["gathered_data"]]
    assert "https://fastapi.tiangolo.com/" in urls
    assert "https://developer.mozilla.org/en-US/docs/Web/HTTP" in urls
    assert "https://www.geeksforgeeks.org/top-python-frameworks/" not in urls


def test_analyzer_agent_penalizes_agency_comparison_blogs_for_programming_queries():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    agent.run_analysis(
        "Сравни FastAPI и Flask для небольшого REST API",
        [
            SearchTask(
                id="task-1",
                description="Сравнение FastAPI и Flask",
                queries=["fastapi vs flask api documentation"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://fastapi.tiangolo.com/async/",
                        "domain": "fastapi.tiangolo.com",
                        "source_quality": "high",
                        "title": "Concurrency and async / await - FastAPI",
                        "content": "Official documentation and reference guide for FastAPI async support, concurrency, and production behavior. " * 15,
                    },
                    {
                        "url": "https://flask.palletsprojects.com/en/stable/extensions/",
                        "domain": "flask.palletsprojects.com",
                        "source_quality": "high",
                        "title": "Flask Extensions",
                        "content": "Official Flask documentation covering extensions and application structure for REST APIs. " * 15,
                    },
                    {
                        "url": "https://www.amplework.com/blog/fastapi-vs-flask-ai-web-framework-comparison/",
                        "domain": "www.amplework.com",
                        "source_quality": "medium",
                        "title": "FastAPI vs Flask: Which Framework Is Best for AI Web Apps?",
                        "content": "This comparison blog discusses which framework to choose and key differences for AI web apps. " * 12,
                    },
                ],
            )
        ],
    )

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    urls = [item["url"] for item in parsed["gathered_data"]]
    assert "https://fastapi.tiangolo.com/async/" in urls
    assert "https://flask.palletsprojects.com/en/stable/extensions/" in urls
    assert "https://www.amplework.com/blog/fastapi-vs-flask-ai-web-framework-comparison/" not in urls


def test_analyzer_agent_prefers_high_quality_reference_sources_over_social_results():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://www.linkedin.com/posts/example",
                        "domain": "www.linkedin.com",
                        "source_quality": "medium",
                        "title": "API Guide",
                        "content": "General API guide content " * 80,
                    },
                    {
                        "url": "https://docs.python.org/3/library/asyncio.html",
                        "domain": "docs.python.org",
                        "source_quality": "high",
                        "title": "Asyncio API Reference",
                        "content": "Official documentation and API reference content " * 50,
                    },
                ],
            )
        ],
    )

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    gathered = parsed["gathered_data"]
    assert gathered[0]["url"] == "https://docs.python.org/3/library/asyncio.html"


def test_analyzer_agent_preserves_source_quality_metadata_in_payload():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://docs.python.org/3/tutorial/",
                        "domain": "docs.python.org",
                        "source_quality": "high",
                        "title": "Python Tutorial",
                        "content": "Useful Python tutorial body " * 50,
                    }
                ],
            )
        ],
    )

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    gathered = parsed["gathered_data"]
    assert gathered[0]["domain"] == "docs.python.org"
    assert gathered[0]["source_quality"] == "high"


def test_analyzer_agent_excludes_low_quality_speculative_sources_from_payload():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    agent.run_analysis(
        "What is new in IT in 2026?",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://thetechandgadgetreviews.com/innovative-gadgets-coming/",
                        "domain": "thetechandgadgetreviews.com",
                        "source_quality": "low",
                        "title": "Innovative Gadgets Coming Soon in 2026",
                        "content": (
                            "These predictions for 2026 describe gadgets that may arrive soon and what to expect "
                            "from future devices. Rumored features could reshape the market."
                        ),
                    }
                ],
            )
        ],
    )

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    assert parsed["gathered_data"] == []


def test_analyzer_agent_keeps_stronger_sources_while_dropping_speculative_noise():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    agent.run_analysis(
        "What is new in IT in 2026?",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://thetechandgadgetreviews.com/innovative-gadgets-coming/",
                        "domain": "thetechandgadgetreviews.com",
                        "source_quality": "low",
                        "title": "Innovative Gadgets Coming Soon in 2026",
                        "content": (
                            "These predictions for 2026 describe gadgets that may arrive soon and what to expect "
                            "from future devices. Rumored features could reshape the market."
                        ),
                    },
                    {
                        "url": "https://www.comptia.org/en-us/blog/top-tech-trends-to-watch-in-2026/",
                        "domain": "www.comptia.org",
                        "source_quality": "high",
                        "title": "Top Tech Trends to Watch in 2026",
                        "content": "Industry report covering AI infrastructure, security, cloud, and enterprise adoption " * 40,
                    },
                ],
            )
        ],
    )

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    gathered = parsed["gathered_data"]
    assert len(gathered) == 1
    assert gathered[0]["url"] == "https://www.comptia.org/en-us/blog/top-tech-trends-to-watch-in-2026/"


def test_analyzer_agent_prefers_reported_news_and_official_announcements_over_trend_roundups():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    agent.run_analysis(
        "Последние новости ИИ и крупные анонсы за сегодня",
        [
            SearchTask(
                id="task-1",
                description="Новости ИИ и официальные анонсы",
                queries=["latest ai news today"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://www.reuters.com/technology/example-story/",
                        "domain": "www.reuters.com",
                        "source_quality": "high",
                        "title": "Reuters reports new AI launch",
                        "content": "Reuters reported the launch and cited company statements and market reactions. " * 18,
                    },
                    {
                        "url": "https://openai.com/index/example-announcement/",
                        "domain": "openai.com",
                        "source_quality": "high",
                        "title": "OpenAI announcement",
                        "content": "Official announcement describing the product release and deployment details. " * 18,
                    },
                    {
                        "url": "https://futureinsights.com/trends-to-watch-in-ai-2026/",
                        "domain": "futureinsights.com",
                        "source_quality": "low",
                        "title": "AI trends to watch in 2026",
                        "content": "This article describes predictions for the AI market and what to expect in the future. " * 10,
                    },
                ],
            )
        ],
    )

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    urls = [item["url"] for item in parsed["gathered_data"]]
    assert "https://www.reuters.com/technology/example-story/" in urls
    assert "https://openai.com/index/example-announcement/" in urls
    assert "https://futureinsights.com/trends-to-watch-in-ai-2026/" not in urls


def test_analyzer_agent_prefers_premium_consumer_tech_sources_over_weak_mobile_listicles():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    agent.run_analysis(
        "Лучшие смартфоны за 2026 год какой лучше купить?",
        [
            SearchTask(
                id="task-1",
                description="Сравни лучшие смартфоны 2026 года",
                queries=["лучшие смартфоны 2026"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://www.gsmarena.com/best_phones_buyers_guide-review-2036.php",
                        "domain": "www.gsmarena.com",
                        "source_quality": "medium",
                        "title": "Best phones buyer's guide",
                        "content": "Our tested review compares battery life, cameras, chipsets, and real-world performance across flagship phones. " * 20,
                    },
                    {
                        "url": "https://www.tomsguide.com/phones/best-phones",
                        "domain": "www.tomsguide.com",
                        "source_quality": "high",
                        "title": "The best phones tested and reviewed",
                        "content": "Editors tested the top phones and compared cameras, displays, and battery life across flagship models. " * 20,
                    },
                    {
                        "url": "https://www.gizbot.com/mobile/features/best-smartphones-you-can-buy-right-now-2026-0001.html",
                        "domain": "www.gizbot.com",
                        "source_quality": "low",
                        "title": "Best smartphones you can buy right now in 2026",
                        "content": "This buying guide lists the best smartphones for every budget and highlights rumored launch expectations. " * 8,
                    },
                    {
                        "url": "https://www.timesnownews.com/technology-science/best-smartphones-to-buy-in-2026-article-123456",
                        "domain": "www.timesnownews.com",
                        "source_quality": "low",
                        "title": "Best smartphones to buy in 2026",
                        "content": "This buyers guide covers the best phones to buy and what to expect from upcoming models this year. " * 8,
                    },
                ],
            )
        ],
    )

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    gathered = parsed["gathered_data"]
    urls = [item["url"] for item in gathered]
    assert "https://www.gsmarena.com/best_phones_buyers_guide-review-2036.php" in urls
    assert "https://www.tomsguide.com/phones/best-phones" in urls
    assert "https://www.gizbot.com/mobile/features/best-smartphones-you-can-buy-right-now-2026-0001.html" not in urls
    assert "https://www.timesnownews.com/technology-science/best-smartphones-to-buy-in-2026-article-123456" not in urls


def test_analyzer_agent_post_processes_sources_heading():
    llm = RecordingLLM(response="Introduction\n\nSources:\n- [S1] https://example.com")
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[{"url": "https://example.com", "title": "Example", "content": "Body"}],
            )
        ],
    )

    assert "## Sources" in result
    assert "Sources:" not in result


def test_analyzer_agent_adds_sources_heading_when_missing():
    llm = RecordingLLM(response="Introduction\n\nConclusion")
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[{"url": "https://example.com", "title": "Example", "content": "Body"}],
            )
        ],
    )

    assert result.endswith("## Sources")


def test_analyzer_agent_rebuilds_sources_from_valid_inline_citations():
    llm = RecordingLLM(
        response=(
            "Introduction [S1] [S9]\n\n"
            "## Sources\n"
            "- [S1] https://wrong.example\n"
            "- [S9] https://wrong.example/two"
        )
    )
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "original prompt",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[{"url": "https://example.com", "title": "Example", "content": "Body"}],
            )
        ],
    )

    assert "[S9]" not in result
    assert "https://wrong.example" not in result
    assert "## Sources\n- [S1] https://example.com" in result


def test_analyzer_agent_retries_once_when_report_language_mismatches_prompt():
    llm = SequentialLLM(
        responses=[
            "## Introduction\nThis report compares APIs. [S1]\n\n## Sources\n- [S1] https://example.com",
            "## Введение\nЭтот отчет сравнивает API. [S1]\n\n## Sources\n- [S1] https://example.com",
        ]
    )
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "Сравни FastAPI и Flask для небольших API",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[{"url": "https://example.com", "title": "Example", "content": "Body"}],
            )
        ],
    )

    assert len(llm.calls) == 2
    assert "wrong language" in llm.calls[1]["user_prompt"]
    assert "## Введение" in result


def test_analyzer_agent_detects_conflicts_from_overlapping_claims():
    llm = RecordingLLM(response="## Introduction\nComparison body. [S1] [S2]\n\n## Conclusion\nDone.")
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "Compare two systems in English",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://example.com/a",
                        "title": "A",
                        "content": "Framework X supports async handlers in production and handles 1000 requests per second reliably.",
                    },
                    {
                        "url": "https://example.com/b",
                        "title": "B",
                        "content": "Framework X does not support async handlers in production and handles 300 requests per second reliably.",
                    },
                ],
            )
        ],
    )

    assert "## Conflicts And Uncertainties" in result
    assert "[S1]" in result
    assert "[S2]" in result


def test_analyzer_agent_passes_detected_conflicts_into_prompt_payload():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    agent.run_analysis(
        "Compare two systems in English",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://example.com/a",
                        "title": "A",
                        "content": "Platform Y has 99 percent uptime in production and supports async jobs.",
                    },
                    {
                        "url": "https://example.com/b",
                        "title": "B",
                        "content": "Platform Y has 92 percent uptime in production and supports async jobs.",
                    },
                ],
            )
        ],
    )

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    assert parsed["detected_conflicts"]
    assert parsed["detected_conflicts"][0]["source_ids"] == ["S1", "S2"]


def test_analyzer_agent_passes_evidence_groups_into_prompt_payload():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    agent.run_analysis(
        "Compare two systems in English",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://example.com/a",
                        "title": "A",
                        "content": "Redis cluster failover replication throughput remains stable in production and offers clear deployment guidance for enterprise teams.",
                    },
                    {
                        "url": "https://example.com/b",
                        "title": "B",
                        "content": "Redis cluster failover replication throughput remains stable in production and includes deployment guidance for enterprise teams with examples.",
                    },
                ],
            )
        ],
    )

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    assert parsed["evidence_groups"]
    assert parsed["evidence_groups"][0]["source_ids"] == ["S1", "S2"]


def test_analyzer_agent_ignores_year_only_or_generic_overlap_as_conflict():
    llm = RecordingLLM(response="## Introduction\nSummary [S1] [S2]\n\n## Conclusion\nDone.")
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "Compare Django and FastAPI in English",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://example.com/a",
                        "title": "A",
                        "content": "In 2026 Django remains a dependable choice for teams building server-rendered applications with strong conventions.",
                    },
                    {
                        "url": "https://example.com/b",
                        "title": "B",
                        "content": "In 2025 FastAPI is a modern async-first framework for API-first services with strong typing and clear contracts.",
                    },
                ],
            )
        ],
    )

    assert "## Conflicts And Uncertainties" not in result


def test_analyzer_agent_requires_more_than_generic_overlap_for_numeric_conflict():
    llm = RecordingLLM(response="report")
    agent = AnalyzerAgent(llm)

    agent.run_analysis(
        "Compare systems in English",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {
                        "url": "https://example.com/a",
                        "title": "A",
                        "content": "Framework overview for Python teams in production mentions 2026 planning guidance and migration paths.",
                    },
                    {
                        "url": "https://example.com/b",
                        "title": "B",
                        "content": "Framework overview for Python teams in production mentions 2025 planning guidance and adoption paths.",
                    },
                ],
            )
        ],
    )

    payload = llm.calls[0]["user_prompt"].split("\n\n", maxsplit=1)[1]
    parsed = json.loads(payload)
    assert parsed["detected_conflicts"] == []


def test_analyzer_agent_adds_report_notes_for_missing_structure_and_citations():
    llm = RecordingLLM(response="Plain body without headings or inline citations.")
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "Compare systems in English",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[{"url": "https://example.com", "title": "Example", "content": "Body"}],
            )
        ],
    )

    assert "## Report Notes" in result
    assert "missing a clear introduction heading" in result
    assert "missing a clear conclusion heading" in result
    assert "does not cite any sources inline" in result
    assert "fewer than two usable sources" in result


def test_analyzer_agent_does_not_add_report_notes_for_well_formed_report():
    llm = RecordingLLM(
        response=(
            "## Introduction\nReport intro [S1] [S2]\n\n"
            "## Conclusion\nReport outro [S1].\n\n"
            "## Sources\n- [S1] https://wrong.example\n- [S2] https://wrong.example/two"
        )
    )
    agent = AnalyzerAgent(llm)

    result = agent.run_analysis(
        "Compare systems in English",
        [
            SearchTask(
                id="task-1",
                description="desc",
                queries=["query"],
                status=TaskStatus.COMPLETED,
                result=[
                    {"url": "https://example.com/1", "title": "Example 1", "content": "Body one"},
                    {"url": "https://example.com/2", "title": "Example 2", "content": "Body two"},
                ],
            )
        ],
    )

    assert "## Report Notes" not in result


def test_decompose_does_not_schedule_failed_tasks(mocker):
    orchestrator = mocker.Mock()
    orchestrator.run_decompose.return_value = [
        {
            "id": "task-1",
            "description": "bad task",
            "queries": ["query"],
            "status": TaskStatus.FAILED,
        }
    ]
    task_store = InMemoryTaskStore()
    service = ResearchService(task_store=task_store, orchestrator=orchestrator)
    response = service.decompose_prompt(
        "test",
        SearchDepth.EASY,
    )

    assert len(response.tasks) == 1
    assert response.tasks[0].status == TaskStatus.FAILED
    assert task_store.get_pending_search_task_jobs() == []


def test_decompose_persists_search_job_for_pending_task(mocker):
    orchestrator = mocker.Mock()
    orchestrator.run_decompose.return_value = [
        {
            "id": "task-1",
            "description": "good task",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    ]
    task_store = InMemoryTaskStore()
    service = ResearchService(task_store=task_store, orchestrator=orchestrator)

    response = service.decompose_prompt("test", SearchDepth.HARD)

    assert response.tasks[0].status == TaskStatus.PENDING
    job = task_store.get_latest_search_task_job("task-1")
    assert job is not None
    assert job.depth == SearchDepth.HARD


def test_start_research_persists_task_ids_in_task_store(mocker):
    orchestrator = mocker.Mock()
    orchestrator.run_decompose.return_value = [
        {
            "id": "task-1",
            "description": "task one",
            "queries": ["query one"],
            "status": TaskStatus.PENDING,
        },
        {
            "id": "task-2",
            "description": "task two",
            "queries": ["query two"],
            "status": TaskStatus.PENDING,
        },
    ]
    task_store = InMemoryTaskStore()
    service = ResearchService(task_store=task_store, orchestrator=orchestrator)

    response = service.start_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
    )

    research = task_store.get_research(response.research_id)
    assert research is not None
    assert research.task_ids == ["task-1", "task-2"]


def test_get_research_status_does_not_rerun_analysis_while_analyzing(mocker):
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    task_store.update_research_status(research.id, ResearchStatus.ANALYZING)
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    analyzer = mocker.Mock()
    service = ResearchService(task_store=task_store, analyzer=analyzer)

    current = service.get_research_status(research.id)

    assert current.status == ResearchStatus.ANALYZING
    analyzer.run_analysis.assert_not_called()


def test_get_research_status_is_read_only_when_tasks_are_done(mocker):
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    analyzer = mocker.Mock()
    service = ResearchService(task_store=task_store, analyzer=analyzer)

    current = service.get_research_status(research.id)

    assert current.status == ResearchStatus.PROCESSING
    analyzer.run_analysis.assert_not_called()


def test_finalize_research_runs_analysis_when_tasks_are_complete(mocker):
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    analyzer = mocker.Mock()
    analyzer.run_analysis.return_value = "final report"
    service = ResearchService(task_store=task_store, analyzer=analyzer)

    finalized = service.finalize_research(research.id)

    assert finalized.status == ResearchStatus.COMPLETED
    assert finalized.final_report == "final report"
    analyzer.run_analysis.assert_called_once()


def test_enqueue_research_finalization_marks_research_analyzing(mocker):
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    analyzer = mocker.Mock()
    service = ResearchService(task_store=task_store, analyzer=analyzer)

    queued, job = service.enqueue_research_finalization(research.id)

    assert queued.status == ResearchStatus.ANALYZING
    assert job is not None
    assert job.status.value == "pending"
    analyzer.run_analysis.assert_not_called()


def test_enqueue_research_finalization_persists_job_record(mocker):
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    analyzer = mocker.Mock()
    service = ResearchService(task_store=task_store, analyzer=analyzer)

    queued, job = service.enqueue_research_finalization(research.id)

    assert queued.status == ResearchStatus.ANALYZING
    assert job is not None
    assert job.research_id == research.id
    assert task_store.get_research_finalize_job(job.id) is not None


def test_process_finalize_job_runs_analysis_and_marks_job_completed(mocker):
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    analyzer = mocker.Mock()
    analyzer.run_analysis.return_value = "background report"
    service = ResearchService(task_store=task_store, analyzer=analyzer)
    _, job = service.enqueue_research_finalization(research.id)

    processed = service.process_finalize_job(job.id)

    assert processed is not None
    assert processed.status.value == "completed"
    assert task_store.get_research(research.id).final_report == "background report"
    analyzer.run_analysis.assert_called_once()


def test_get_research_finalize_job_returns_persisted_job(mocker):
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "done task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    analyzer = mocker.Mock()
    service = ResearchService(task_store=task_store, analyzer=analyzer)
    _, job = service.enqueue_research_finalization(research.id)

    fetched = service.get_research_finalize_job(job.id)

    assert fetched is not None
    assert fetched.id == job.id


def test_get_latest_research_finalize_job_returns_most_recent_job():
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    older = task_store.add_research_finalize_job(research.id)
    newer = task_store.add_research_finalize_job(research.id)

    fetched = ResearchService(task_store=task_store).get_latest_research_finalize_job(research.id)

    assert fetched is not None
    assert fetched.id == newer.id
    assert fetched.id != older.id
    assert fetched.research_id == research.id


def test_process_search_task_job_marks_job_completed(mocker):
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )
    job = task_store.add_search_task_job("task-1", SearchDepth.MEDIUM.value)
    service = ResearchService(task_store=task_store)
    run_search_task = mocker.patch.object(service, "run_search_task")

    processed = service.process_search_task_job(job.id)

    assert processed is not None
    assert processed.status == SearchJobStatus.COMPLETED
    run_search_task.assert_called_once_with("task-1", SearchDepth.MEDIUM)


@pytest.mark.parametrize(
    ("depth", "expected_limit", "expected_results_per_query", "expected_candidate_urls"),
    [
        (SearchDepth.EASY, 5, 8, 12),
        (SearchDepth.MEDIUM, 12, 12, 24),
        (SearchDepth.HARD, 20, 16, 36),
    ],
)
def test_run_search_task_uses_depth_profile_source_limit(
    mocker,
    depth,
    expected_limit,
    expected_results_per_query,
    expected_candidate_urls,
):
    service = ResearchService(task_store=InMemoryTaskStore())
    run_task = mocker.patch("src.agents.search.SearchAgent.run_task")
    init = mocker.patch("src.services.research_service.SearchAgent.__init__", return_value=None)

    service.run_search_task("task-1", depth)

    init.assert_called_once_with(
        task_store=service.task_store,
        max_sources=expected_limit,
        search_results_per_query=expected_results_per_query,
        max_candidate_urls=expected_candidate_urls,
        extraction_concurrency=4,
        extraction_timeout_seconds=12,
    )
    run_task.assert_called_once_with("task-1")


def test_depth_profiles_keep_medium_as_default_balanced_mode():
    profile = get_depth_profile(SearchDepth.MEDIUM)

    assert profile["label"] == "Balanced"
    assert profile["task_count"] == 4
    assert profile["source_limit"] == 12


def test_process_finalize_job_marks_missing_job_as_none():
    service = ResearchService(task_store=InMemoryTaskStore())

    assert service.process_finalize_job("missing") is None


def test_process_search_task_job_marks_missing_job_as_none():
    service = ResearchService(task_store=InMemoryTaskStore())

    assert service.process_search_task_job("missing") is None


def test_get_latest_search_task_job_returns_persisted_job():
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )
    job = task_store.add_search_task_job("task-1", SearchDepth.EASY.value)
    service = ResearchService(task_store=task_store)

    fetched = service.get_latest_search_task_job("task-1")

    assert fetched is not None
    assert fetched.id == job.id


def test_get_queue_metrics_reports_dead_letter_counts():
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )
    search_job = task_store.add_search_task_job("task-1", SearchDepth.EASY.value, max_attempts=1)
    task_store.claim_next_search_task_job()
    task_store.record_search_task_job_failure(search_job.id, "boom")

    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    finalize_job = task_store.add_research_finalize_job(research.id, max_attempts=1)
    task_store.claim_next_research_finalize_job()
    task_store.record_research_finalize_job_failure(finalize_job.id, "boom")

    metrics = ResearchService(task_store=task_store).get_queue_metrics()

    assert metrics.dead_letter_search_jobs == 1
    assert metrics.dead_letter_finalize_jobs == 1
    assert metrics.extraction_metrics.attempts == 0


def test_get_worker_heartbeat_returns_latest_value():
    task_store = InMemoryTaskStore()
    task_store.upsert_worker_heartbeat(
        "job-worker",
        processed_jobs=2,
        status="busy",
        extraction_metrics={"attempts": 3, "success_count": 2},
    )

    heartbeat = ResearchService(task_store=task_store).get_worker_heartbeat("job-worker")

    assert heartbeat is not None
    assert heartbeat.worker_name == "job-worker"
    assert heartbeat.processed_jobs == 2
    assert heartbeat.extraction_metrics.attempts == 3
    assert heartbeat.extraction_metrics.success_count == 2


def test_get_queue_metrics_aggregates_extraction_metrics_from_worker_heartbeats():
    task_store = InMemoryTaskStore()
    task_store.upsert_worker_heartbeat(
        "job-worker",
        processed_jobs=1,
        status="busy",
        extraction_metrics={"attempts": 3, "success_count": 2, "failure_count": 1},
    )
    task_store.upsert_worker_heartbeat(
        "job-worker-2",
        processed_jobs=1,
        status="busy",
        extraction_metrics={"attempts": 4, "success_count": 3, "empty_count": 1},
    )

    metrics = ResearchService(task_store=task_store).get_queue_metrics()

    assert metrics.extraction_metrics.attempts == 7
    assert metrics.extraction_metrics.success_count == 5
    assert metrics.extraction_metrics.failure_count == 1
    assert metrics.extraction_metrics.empty_count == 1
    assert metrics.extraction_metrics.success_rate_percent == 71.4


def test_extraction_metrics_derives_rates_and_averages():
    metrics = ExtractionMetrics(
        attempts=4,
        success_count=3,
        total_download_ms=40,
        total_extract_ms=20,
        total_post_process_ms=8,
        total_total_ms=68,
    )

    assert metrics.success_rate_percent == 75.0
    assert metrics.avg_download_ms == 10.0
    assert metrics.avg_extract_ms == 5.0
    assert metrics.avg_post_process_ms == 2.0
    assert metrics.avg_total_ms == 17.0


def test_task_summary_preserves_search_metrics():
    task_store = InMemoryTaskStore()
    task_store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "search_metrics": {
                "candidate_count": 6,
                "extraction_attempts": 4,
                "extraction_success_count": 3,
                "extraction_failure_count": 1,
                "selected_source_count": 2,
                "avg_content_chars": 512.5,
            },
        }
    )

    summary = ResearchService(task_store=task_store).get_task_summary("task-1")

    assert summary.search_metrics.candidate_count == 6
    assert summary.search_metrics.extraction_attempts == 4
    assert summary.search_metrics.extraction_success_count == 3
    assert summary.search_metrics.extraction_failure_count == 1
    assert summary.search_metrics.selected_source_count == 2


def test_research_summary_aggregates_task_search_metrics():
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1", "task-2"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "task 1",
            "queries": ["query 1"],
            "status": TaskStatus.COMPLETED,
            "search_metrics": {
                "candidate_count": 6,
                "extraction_attempts": 4,
                "extraction_success_count": 3,
                "extraction_failure_count": 1,
                "selected_source_count": 2,
            },
        }
    )
    task_store.add_task(
        {
            "id": "task-2",
            "research_id": research.id,
            "description": "task 2",
            "queries": ["query 2"],
            "status": TaskStatus.COMPLETED,
            "search_metrics": {
                "candidate_count": 5,
                "extraction_attempts": 3,
                "extraction_success_count": 2,
                "extraction_failure_count": 1,
                "selected_source_count": 2,
            },
        }
    )

    summary = ResearchService(task_store=task_store).get_research_summary(research.id)

    assert summary.total_candidates == 11
    assert summary.total_extraction_attempts == 7
    assert summary.total_extraction_success_count == 5
    assert summary.total_extraction_failure_count == 2
    assert summary.total_selected_source_count == 4


def test_enqueue_research_finalization_fails_immediately_when_all_tasks_failed(mocker):
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "failed task",
            "queries": ["query"],
            "status": TaskStatus.FAILED,
        }
    )
    analyzer = mocker.Mock()
    service = ResearchService(task_store=task_store, analyzer=analyzer)

    finalized, job = service.enqueue_research_finalization(research.id)

    assert finalized.status == ResearchStatus.FAILED
    assert finalized.final_report == "All tasks failed."
    assert job is None
    analyzer.run_analysis.assert_not_called()


def test_finalize_research_rejects_incomplete_tasks():
    task_store = InMemoryTaskStore()
    research = task_store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    task_store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "pending task",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )
    service = ResearchService(task_store=task_store)

    with pytest.raises(HTTPException) as exc_info:
        service.finalize_research(research.id)

    assert exc_info.value.status_code == 409
