#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

echo "Running style checks"
flake8

echo "Running unit tests"
pytest

echo "Running coverage tests"
pytest --cov=workflow --cov-report html --cov-fail-under=90
