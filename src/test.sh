#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

## Coverage 6: coverage run --data-file=/tmp/.coveragerc …
#export COVERAGE_FILE=/tmp/.coverage
#
#echo "Running unit tests"
#coverage run --source=./gobworkflow -m pytest tests/
#
#echo "Coverage report"
#coverage report --show-missing --fail-under=100
#
#echo "Running style checks"
#flake8 ./gobworkflow
#
echo() {
   builtin echo -e "$@"
}

export COVERAGE_FILE="/tmp/.coverage"

# Add files to pass through Flake8, Black and mypy checks.
CLEAN_FILES=(
  gobworkflow/__init__.py
)

# Uncomment files to pass through Flake8 & Black checks. Move mypy clean files to CLEAN_FILES.
DIRTY_FILES=(
  gobworkflow/config.py
  gobworkflow/start/__init__.py
  gobworkflow/start/__main__.py
  gobworkflow/storage/auto_reconnect_wrapper.py
  gobworkflow/storage/__init__.py
  gobworkflow/storage/storage.py
  gobworkflow/workflow/tree.py
  gobworkflow/workflow/hooks.py
  gobworkflow/workflow/config.py
  gobworkflow/workflow/__init__.py
  gobworkflow/workflow/start.py
  gobworkflow/workflow/jobs.py
  gobworkflow/workflow/workflow.py
  gobworkflow/task/queue.py
  gobworkflow/task/__init__.py
  gobworkflow/__main__.py
  gobworkflow/heartbeats.py
)

# Combine CLEAN_FILES and DIRTY_FILES.
FILES=( "${CLEAN_FILES[@]}" "${DIRTY_FILES[@]}" )


echo "Running mypy on non-dirty files"
mypy "${CLEAN_FILES[@]}"

echo "\nRunning unit tests"
coverage run --source=gobworkflow -m pytest

echo "Coverage report"
coverage report --fail-under=100

echo "\nCheck if Black finds no potential reformat fixes"
black --check --diff "${FILES[@]}"

echo "\nCheck for potential import sort"
isort --check --diff --src-path=gobworkflow "${FILES[@]}"

echo "\nRunning Flake8 style checks"
flake8 "${FILES[@]}"

echo "\nChecks complete"
