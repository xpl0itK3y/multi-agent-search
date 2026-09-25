#!/usr/bin/env bash
# Apply the default-branch ruleset in .github/rulesets/main.json to the GitHub repository.
#
# Needs an authenticated GitHub CLI (`gh auth login`) for an account with admin rights on
# the repository. Idempotent: an existing ruleset with the same name is updated in place.
#
#   scripts/apply_branch_protection.sh                 # repository of the current checkout
#   scripts/apply_branch_protection.sh owner/repo      # explicit repository
set -Eeuo pipefail

ruleset_file="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.github/rulesets/main.json"
repo="${1:-$(gh repo view --json nameWithOwner --jq .nameWithOwner)}"
name="$(sed -n 's/^  "name": "\(.*\)",$/\1/p' "$ruleset_file")"

if [[ -z "$name" ]]; then
  echo "Could not read the ruleset name from $ruleset_file" >&2
  exit 1
fi

existing_id="$(gh api "repos/$repo/rulesets" --paginate --jq ".[] | select(.name == \"$name\") | .id")"

if [[ -n "$existing_id" ]]; then
  gh api --method PUT "repos/$repo/rulesets/$existing_id" --input "$ruleset_file" --silent
  echo "Updated ruleset '$name' ($existing_id) on $repo"
else
  new_id="$(gh api --method POST "repos/$repo/rulesets" --input "$ruleset_file" --jq .id)"
  echo "Created ruleset '$name' ($new_id) on $repo"
fi
