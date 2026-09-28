"""The default-branch ruleset (.github/rulesets/main.json) must stay in step with CI.

A required status check is matched by the job's display name. If a job is renamed or a new
gate is added without updating the ruleset, GitHub either waits forever for a check that no
longer exists (blocking every merge) or silently stops requiring the new gate. These tests
turn that drift into a CI failure instead.
"""
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
RULESET = ROOT / ".github" / "rulesets" / "main.json"
WORKFLOWS = ROOT / ".github" / "workflows"
APPLY_SCRIPT = ROOT / "scripts" / "apply_branch_protection.sh"
GITHUB_ACTIONS_APP_ID = 15368


def _ruleset() -> dict:
    return json.loads(RULESET.read_text(encoding="utf-8"))


def _rule(ruleset: dict, rule_type: str) -> dict:
    matches = [rule for rule in ruleset["rules"] if rule["type"] == rule_type]
    assert len(matches) == 1, f"expected exactly one {rule_type!r} rule"
    return matches[0]


def _pull_request_check_names() -> set[str]:
    names = set()
    for path in sorted(WORKFLOWS.glob("*.yml")):
        workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
        # PyYAML reads a bare `on:` key as the boolean True.
        triggers = workflow.get("on", workflow.get(True)) or {}
        if "pull_request" not in triggers:
            continue
        for job_id, job in workflow["jobs"].items():
            names.add(job.get("name", job_id))
    return names


def test_required_checks_are_exactly_the_pull_request_ci_jobs():
    checks = _rule(_ruleset(), "required_status_checks")["parameters"]["required_status_checks"]

    assert {check["context"] for check in checks} == _pull_request_check_names()
    # Pin every check to GitHub Actions so a commit status posted by anyone with write
    # access cannot satisfy it.
    assert {check.get("integration_id") for check in checks} == {GITHUB_ACTIONS_APP_ID}


def test_ruleset_protects_the_default_branch_without_bypass():
    ruleset = _ruleset()

    assert ruleset["target"] == "branch"
    assert ruleset["enforcement"] == "active"
    assert ruleset["conditions"]["ref_name"]["include"] == ["~DEFAULT_BRANCH"]
    assert ruleset["bypass_actors"] == []  # admins are held to the same gate
    _rule(ruleset, "deletion")
    _rule(ruleset, "non_fast_forward")


def test_merges_go_through_an_up_to_date_pull_request():
    ruleset = _ruleset()
    pull_request = _rule(ruleset, "pull_request")["parameters"]
    status_checks = _rule(ruleset, "required_status_checks")["parameters"]

    assert pull_request["required_approving_review_count"] == 0  # two-person team: self-merge on green CI
    assert pull_request["dismiss_stale_reviews_on_push"] is True
    assert pull_request["required_review_thread_resolution"] is True
    assert status_checks["strict_required_status_checks_policy"] is True
    assert status_checks["do_not_enforce_on_create"] is False


def test_apply_script_reads_the_committed_ruleset():
    script = APPLY_SCRIPT.read_text(encoding="utf-8")

    assert ".github/rulesets/main.json" in script
    # The script extracts the name with a line-anchored sed; keep the key where it looks.
    assert f'  "name": "{_ruleset()["name"]}",' in RULESET.read_text(encoding="utf-8").splitlines()
