#!/usr/bin/env bash
set -euo pipefail

if [[ "${OPX_SKIP_PRE_COMMIT_CHECKS:-0}" == "1" ]]; then
  echo "Skipping local quality checks because OPX_SKIP_PRE_COMMIT_CHECKS=1"
  exit 0
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

python_bin="${OPX_PRE_COMMIT_PYTHON:-}"
if [[ -z "$python_bin" ]]; then
  if [[ -x "$repo_root/.venv/bin/python" ]]; then
    python_bin="$repo_root/.venv/bin/python"
  else
    python_bin="python3"
  fi
fi

if ! command -v "$python_bin" >/dev/null 2>&1; then
  echo "Unable to find Python interpreter for local quality checks: $python_bin" >&2
  exit 1
fi

echo "Running local quality checks with: $python_bin"
echo "1/2 pytest -q"
"$python_bin" -m pytest -q

echo "2/2 pylint"
tracked_python_files="$(git ls-files '*.py')"
if [[ -z "$tracked_python_files" ]]; then
  echo "No tracked Python files found for pylint."
  exit 0
fi

set +u
# shellcheck disable=SC2086
"$python_bin" -m pylint $tracked_python_files
