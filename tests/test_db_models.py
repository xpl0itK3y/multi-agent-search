from src.db import Base


def test_db_metadata_contains_core_tables():
    table_names = set(Base.metadata.tables.keys())

    assert {
        "researches",
        "search_tasks",
        "search_results",
        "research_finalize_jobs",
    } <= table_names
