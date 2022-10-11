MYPY_OPTIONS = --ignore-missing-imports --disallow-untyped-calls --disallow-untyped-defs --disallow-incomplete-defs

.PHONY: install
install:
	poetry install

.PHONY: notebook
notebook:
	poetry run jupyter-lab

.PHONY: ingest
ingest:
	poetry run python delta-demo/ingest.py

.PHONY: requirements
requirements:
	poetry export -f requirements.txt --output requirements.txt --dev
