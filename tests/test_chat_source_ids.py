"""C7-1: chat follow-up searches number their sources in a chat-only [Sn] table.

Each turn used to rebuild its pool from the report's sources only and number new search
results from the report's max id, so the next search handed the same id to a different
URL while the history the model reads still cited the old one, and a turn without a
search could not link an id cited from history at all."""

from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth, TaskStatus, TaskUpdate
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


def _research(store):
    research = store.add_research(ResearchRequest(prompt="battery market", depth=SearchDepth.EASY), task_ids=["task-1"])
    store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "battery market",
            "queries": ["battery market"],
            "status": TaskStatus.COMPLETED,
            "result": [
                {"url": "https://report.example/1", "title": "R1", "content": "battery market overview"},
                {"url": "https://report.example/2", "title": "R2", "content": "battery supply chain"},
            ],
        }
    )
    store.merge_research_graph_state(
        research.id,
        {
            "canonical_sources": [
                {"source_id": "S1", "url": "https://report.example/1", "title": "R1"},
                {"source_id": "S2", "url": "https://report.example/2", "title": "R2"},
            ]
        },
    )
    store.update_research_status(research.id, ResearchStatus.COMPLETED, "Report [S1] [S2].")
    return research


def _search_results(store, mocker, results_by_turn):
    """Patch the chat mini-search's SearchAgent to store the next turn's results."""
    turns = iter(results_by_turn)

    def fake_run_task(self, task_id):
        store.update_task(task_id, TaskUpdate(status=TaskStatus.COMPLETED, result=next(turns)))

    mocker.patch("src.agents.search.SearchAgent.run_task", fake_run_task)


def _turn(service, research_id, question):
    """One chat turn as the chat routes run it: answer, then store both messages."""
    answer = service.generate_research_answer(research_id, question)
    service.append_research_message(research_id, "user", question)
    service.append_research_message(research_id, "assistant", answer.content, answer.sources)
    return answer


def test_chat_source_ids_stay_stable_across_turns_and_history_ids_keep_their_urls(mocker):
    store = InMemoryTaskStore()
    research = _research(store)
    chat = mocker.Mock()
    chat.answer.side_effect = [
        "Turn one fact [S3].",
        "Turn two fact [S4], unlike turn one [S3].",
        "Both follow-ups hold: [S3] and [S4].",
    ]
    service = ResearchService(task_store=store, chat_agent=chat)
    needs_search = mocker.patch.object(service, "_question_needs_search", side_effect=[True, True, False])
    _search_results(
        store,
        mocker,
        [
            [{"url": "https://turn1.example/A", "title": "A", "content": "sodium cells first turn"}],
            [
                {"url": "https://turn1.example/A", "title": "A", "content": "sodium cells first turn"},
                {"url": "https://turn2.example/B", "title": "B", "content": "solid state second turn"},
            ],
        ],
    )

    first = _turn(service, research.id, "What about sodium cells?")
    second = _turn(service, research.id, "And solid state?")
    third = _turn(service, research.id, "Summarise both follow-ups")

    assert {s.source_id: s.url for s in first.sources}["S3"] == "https://turn1.example/A"
    # The second search found A again and B: A keeps S3, B gets the next id, not S3.
    sent = {s["source_id"]: s["url"] for s in chat.answer.call_args_list[1].args[2]}
    assert sent["S3"] == "https://turn1.example/A"
    assert sent["S4"] == "https://turn2.example/B"
    assert {s.source_id: s.url for s in second.sources}["S4"] == "https://turn2.example/B"
    # No search on the third turn: ids cited from history still resolve to their URLs.
    assert needs_search.call_count == 3
    by_id = {s.source_id: s.url for s in third.sources}
    assert by_id["S3"] == "https://turn1.example/A"
    assert by_id["S4"] == "https://turn2.example/B"
    # Chat sources never become report sources.
    state = store.get_research(research.id).graph_state
    assert [s["source_id"] for s in state["canonical_sources"]] == ["S1", "S2"]
    assert [s.source_id for s in service.get_research_sources(research.id)] == ["S1", "S2"]
    assert [s["url"] for batch in state["chat_sources"] for s in batch["sources"]] == [
        "https://turn1.example/A",
        "https://turn2.example/B",
    ]
    assert all("content" not in s for batch in state["chat_sources"] for s in batch["sources"])


def test_earlier_chat_sources_count_as_coverage_and_keep_their_content(mocker):
    store = InMemoryTaskStore()
    research = _research(store)
    chat = mocker.Mock()
    chat.answer.return_value = "Sodium cells [S3]."
    service = ResearchService(task_store=store, chat_agent=chat)
    _search_results(
        store,
        mocker,
        [[{"url": "https://turn1.example/A", "title": "A", "content": "sodium cells lower cost chemistry"}]],
    )
    search = mocker.spy(service, "_mini_search_for_chat")

    _turn(service, research.id, "What about sodium cells chemistry?")
    _turn(service, research.id, "Explain sodium cells chemistry again")

    # The second question is covered by the first search's source: no second paid search.
    assert search.call_count == 1
    sent = {s["source_id"]: s for s in chat.answer.call_args_list[1].args[2]}
    assert sent["S3"]["url"] == "https://turn1.example/A"
    assert "sodium cells lower cost chemistry" in sent["S3"]["content"]


def test_concurrent_chat_searches_get_distinct_ids(mocker):
    # Both turns read the table before either search stored anything: the ids come from
    # the batch order under the row lock, so the two turns cannot hand out the same id.
    store = InMemoryTaskStore()
    research = _research(store)
    chat = mocker.Mock()
    chat.answer.side_effect = lambda question, *args, **kwargs: f"{question} [S3] [S4]"
    service = ResearchService(task_store=store, chat_agent=chat)
    mocker.patch.object(service, "_question_needs_search", return_value=True)
    searched = []
    answers = {}

    def fake_mini_search(research_id, question, depth):
        if question == "first":
            # The second turn runs its whole search while the first one is still searching.
            answers["second"] = service.generate_research_answer(research_id, "second")
            return [{"url": "https://first.example/A", "title": "A", "content": "first"}]
        searched.append(question)
        return [{"url": "https://second.example/B", "title": "B", "content": "second"}]

    mocker.patch.object(service, "_mini_search_for_chat", side_effect=fake_mini_search)

    answers["first"] = service.generate_research_answer(research.id, "first")

    assert searched == ["second"]
    first = {s.url: s.source_id for s in answers["first"].sources}
    second = {s.url: s.source_id for s in answers["second"].sources}
    assert second["https://second.example/B"] == "S3"
    assert first["https://first.example/A"] == "S4"
    # Each answer links the other's id to the other's URL, never to its own.
    assert first["https://second.example/B"] == "S3"


def test_chat_ids_the_report_takes_over_are_left_to_the_report():
    service = ResearchService(task_store=InMemoryTaskStore())
    batches = [{"floor": 2, "sources": [{"url": "https://chat.example/A"}, {"url": "https://chat.example/B"}]}]
    report_pool = [{"source_id": f"S{n}", "url": f"https://report.example/{n}"} for n in (1, 2, 3)]

    table = service._chat_source_table(batches, report_pool, {})

    assert [(s["source_id"], s["url"]) for s in table] == [("S4", "https://chat.example/B")]


def test_a_full_chat_table_stops_new_chat_searches(mocker):
    store = InMemoryTaskStore()
    research = _research(store)
    full = [{"floor": 2, "sources": [{"url": f"https://chat.example/{n}"} for n in range(200)]}]
    store.merge_research_graph_state(research.id, {"chat_sources": full})
    chat = mocker.Mock()
    chat.answer.return_value = "From what is there [S202]."
    service = ResearchService(task_store=store, chat_agent=chat)
    mocker.patch.object(service, "_question_needs_search", return_value=True)
    search = mocker.patch.object(service, "_mini_search_for_chat")

    answer = service.generate_research_answer(research.id, "anything new?")

    search.assert_not_called()
    assert {s.source_id: s.url for s in answer.sources}["S202"] == "https://chat.example/199"
