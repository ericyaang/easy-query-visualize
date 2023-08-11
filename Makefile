.PHONY: env activate install packages

env:
	@echo "Creating virtual environment"
	python -m venv .venv

activate:
	@echo "Activating virtual environment"
	powershell -noexit -executionpolicy bypass .venv/Scripts/activate.ps1

install:
	@echo "Installing pip, setuptools and poetry"
	python -m pip install --upgrade pip setuptools poetry --no-cache-dir

init:
	@echo "Initializing project with Poetry"
	poetry init -n

packages:
	@echo "Installing specified packages with poetry"
	poetry add streamlit duckdb python-dotenv pandas fastparquet
