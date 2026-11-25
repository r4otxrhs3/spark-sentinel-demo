#!/usr/bin/env bash
# Open one of the demo pull requests on GitHub.
#
#   ./scripts/open_demo_pr.sh slow-udf        # the bad PR: a row-by-row Python UDF
#   ./scripts/open_demo_pr.sh no-broadcast    # a setting that turns off broadcast joins
#   ./scripts/open_demo_pr.sh rename-column   # harmless: Sentinel should stay quiet
#
# Needs git and, to open the PR itself, the GitHub CLI (gh auth login). Without gh it pushes
# the branch and prints the link to open the PR by hand.
set -euo pipefail

name="${1:-}"
case "$name" in
  slow-udf)
    title="Share the size-band rule with the pricing service"
    body="Moves the size-band logic into a Python function, the same one the pricing service uses, so the two can't drift apart." ;;
  no-broadcast)
    title="Never broadcast joins, to avoid driver OOM"
    body="A broadcast of a big table ran the driver out of memory once. This turns broadcast joins off." ;;
  rename-column)
    title="Rename orders to order_count in the revenue report"
    body="The dashboard team asked for a clearer column name. No logic changes." ;;
  *)
    echo "usage: $0 slow-udf|no-broadcast|rename-column" >&2
    exit 2 ;;
esac

cd "$(dirname "$0")/.."
if [ -n "$(git status --porcelain)" ]; then
  echo "Commit or stash your changes first." >&2
  exit 1
fi

git switch main
git pull --ff-only
branch="demo/$name-$(date +%Y%m%d-%H%M%S)"
git switch -c "$branch"
git apply "demo-prs/$name.patch"
git commit -am "$title"
git push -u origin "$branch"

if command -v gh >/dev/null 2>&1; then
  gh pr create --base main --head "$branch" --title "$title" --body "$body"
else
  repo_url="$(git remote get-url origin | sed -e 's#git@github.com:#https://github.com/#' -e 's#\.git$##')"
  echo "Open the pull request here: $repo_url/compare/main...$branch?expand=1"
fi
git switch main
