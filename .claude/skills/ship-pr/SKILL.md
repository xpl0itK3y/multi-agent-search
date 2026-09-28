---
name: ship-pr
description: How work lands in multi-agent-search — branch from an up-to-date main, commit in this repo's Conventional Commits style, verify, push, open a pull request with a useful description, and follow the six required CI checks of the protected main branch until they are green (with or without the gh CLI). Use it when the user asks to commit, push, "open a PR", "запушь и открой PR", merge main into a branch, fix a red CI check on a PR, or asks what the branch protection requires.
---

# Branch, commit, push, pull request

Push, open pull requests and merge **only when the user asks**. Committing locally as you finish logical steps is fine. A green PR that nobody asked you to merge is reported, not merged. Never force-push `main`, never rewrite its history, and never skip hooks (`--no-verify`).

## Branch

```bash
git fetch origin
git switch -c <type>/<topic> origin/main      # e.g. feat/account-recovery, fix/wave-5-post-merge-audit, chore/main-branch-protection
```

- Start every piece of work from a fresh `origin/main`.
- `main` is protected by the ruleset in `.github/rulesets/main.json`:
  - changes land only through a PR;
  - no deletion or force-push;
  - the six CI checks must pass on a branch that is **up to date with main**;
  - review conversations must be resolved;
  - **no approval is required**, so the author can merge their own green PR (you merge only when the user asks);
  - there are no bypass actors.
- When `main` moves while a PR is open, bring it in with a merge rather than rebasing a pushed branch: `git fetch origin && git merge origin/main -m "Merge main into <branch>"`.
- The ruleset only takes effect once a repository admin has applied it with `scripts/apply_branch_protection.sh` (GitHub does not read the file by itself). If checks are not enforced, that is why.

## Commits

Conventional Commits with a scope, in the imperative, describing what changes **for the user or the system**:

```
fix(web): keep the view mounted across a ?tab= switch
feat(web): admin graph on Pointer Events: 1:1 drag, pinch, press states
fix(a11y): name the research follow-up send button 'send question'
style(web): status tokens, press and tabular figures across the report
perf(web): the mobile drawer slides on transform only
refactor(web): delete the unused ProgressTrace and AgentNodeCard components
test(web): guard that smooth scrolling goes through lib/motion.ts
feat(i18n): add send, fit-view and danger-zone hint strings
```

- **Types:** `feat`, `fix`, `style`, `perf`, `refactor`, `test`, `docs`, `chore`, `ci`.
- **Scopes:** `web`, `a11y`, `i18n`, `api`, `auth`, `db`, `worker`, `graph`, `ops`, `ci`, … Pick the area a reader would search for.
- **Subject:** lowercase after the colon, no trailing period, about 72 characters at most.
- **Body:** say *why*, and what a reviewer could not guess from the diff: the failure it fixes, the trade-off, the follow-up left open.
- **One logical change per commit.** A fix and its test go together; an unrelated cleanup goes in its own commit.
- Add the attribution trailer your environment specifies, if any.

## Before pushing

1. **Run the checks** (`run-checks` skill): `ci_local.sh all`, plus `postgres`/`smoke` when persistence changed, plus screenshots (`ui-screenshots`) for visible UI changes.
2. **`git status` is clean and the diff holds only intended files.** Nothing from temp output, no screenshots, no `seed_info.json`, no `.env`, no `web/dist`.
3. **Update the documentation that changes with the code:** README sections (settings, migrations, endpoints, operations), `.env.example`, and `docs/improvement-plan.md` §8 when you close or advance one of its items. Its IDs look like `SEC-IDOR`, `JOB-RETRY` or `FE-CHAT-CSRF`.

## Push and open the PR

```bash
git push -u origin <branch>
```

- **Files.** Write the PR body and payload **outside the repo**, in a temp or scratch directory, so `git status` stays clean.
- **With the GitHub CLI:** `gh pr create --base main --head <branch> --title "…" --body-file "$TMP/pr.md"`.
- **Without gh:** use the REST API with the credential git already has. Never print or log the token.

```bash
TOKEN=$(printf 'protocol=https\nhost=github.com\n\n' | GIT_TERMINAL_PROMPT=0 git credential fill | sed -n 's/^password=//p')
curl -s -X POST -H "Authorization: token $TOKEN" -H "Accept: application/vnd.github+json" \
  https://api.github.com/repos/<owner>/<repo>/pulls --data-binary @"$TMP/pr.json"   # {"title","head","base":"main","body"}
unset TOKEN
```

- **Build `pr.json` with a JSON library** (`python -c 'import json…'`), not string concatenation: the body has quotes and newlines. `GIT_TERMINAL_PROMPT=0` makes a missing credential fail instead of waiting for input.
- **A 422 "A pull request already exists"** means one is open for the branch, perhaps created from GitHub's banner. Find it (`GET /repos/<owner>/<repo>/pulls?head=<owner>:<branch>`) and update its title and body with `PATCH /pulls/<n>` instead of opening a second one.
- **On WSL without a Linux toolchain,** use `git.exe` and the repo's `.venv/Scripts/python.exe`. Windows Python cannot open `/mnt/...` or `/tmp/...` paths given as arguments: convert them with `wslpath -w`, or keep the JSON file on a Windows drive.
- **Title and body** are in the language the user works in (recent PRs here are in Russian). The body has these sections:
  - **Зачем / Why**: the problem, in a few sentences.
  - **Что изменилось / What changed**: grouped by area. Name real bugs found on the way explicitly.
  - **Проверка / Verification**: the commands you ran and their results (test counts, "vue-tsc clean", screenshots taken).
  - **Не проверено / Not verified**: anything that was not checked, such as real devices, live LLM runs or production data.
  - **Оставшееся / Follow-ups**: known open issues.
  - The trailer your environment specifies, if any.

## Follow CI

- **The required checks:**
  - from Quality Gates: `Backend unit tests`, `Frontend typecheck and tests`, `Native Rust tests`, `Offline evaluation gate`, `Prometheus alert rules`;
  - from Postgres Smoke: `Postgres smoke`.
- **Watch them** with `gh pr checks <n> --watch`, or poll the Actions API: `GET /repos/<owner>/<repo>/actions/runs?branch=<branch>&event=pull_request&head_sha=<pushed sha>`.
  - Without `head_sha`, older runs of the branch come back too, and an old green run can pass for the current one.
  - Take the newest run per workflow and wait until both are `completed`. The smoke job takes a few minutes.
- **A red check:**
  1. Open the failing job's log: `gh run view <run_id> --log-failed`. Without gh, list the jobs with `GET /repos/<owner>/<repo>/actions/runs/<run_id>/jobs`, then fetch `GET /repos/<owner>/<repo>/actions/jobs/<job_id>/logs` with `curl -L`, because it redirects to a download URL.
  2. Reproduce locally with the matching `ci_local.sh` mode.
  3. Fix it in a new commit and push. Never weaken the check to get green: no skipped tests, no raised tolerances, no rebaselined eval without a reason stated in the commit.
- **Report back:** the PR URL and the final state of the checks, as they are.
