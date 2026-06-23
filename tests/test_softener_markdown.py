"""The claim-verifier softener must never touch markdown structure. A flagged table row or list
item that got a hedging prefix turned into a stray cell / 'По имеющимся данным, - …' and broke
the table's markdown so it stopped rendering (seen in a real report)."""
from src.agents.claim_verifier import ClaimVerifierAgent


def test_softener_leaves_table_and_list_rows_intact():
    cv = ClaimVerifierAgent()
    prose = "Это утверждение без достаточной поддержки источниками."
    table_row = "| **Физический носитель** | Джозефсоновский переход [S4] |"
    list_item = "- Это слабо подтверждённое утверждение."
    report = f"{prose}\n{table_row}\n{list_item}"
    issue = [prose, table_row, list_item]

    out, _ = cv.verify_and_downgrade(report, "ru", [], issue, max_softened=10)
    lines = out.splitlines()
    assert lines[0].startswith("По имеющимся данным,")  # prose IS hedged
    assert lines[1] == table_row  # table row untouched — no stray cell
    assert lines[2] == list_item  # list item untouched


def test_absolute_term_softening_skips_table_cells():
    cv = ClaimVerifierAgent()
    table_row = "| Лучший вариант | всегда стабилен |"  # absolute words, but inside a table
    out, _ = cv.verify_and_downgrade(table_row, "ru", [], [], max_softened=10)
    assert out == table_row
