# Makefile for E-Commerce Data Pipeline

.PHONY: help install test clean run generate-data format lint

help:
	@echo "Available commands:"
	@echo "  make install        - Install dependencies"
	@echo "  make test          - Run tests with coverage"
	@echo "  make clean         - Clean generated files"
	@echo "  make run           - Run the pipeline"
	@echo "  make generate-data - Generate sample data"
	@echo "  make format        - Format code with black"
	@echo "  make lint          - Run linting checks"

install:
	pip install -r requirements.txt
	pip install -e .

test:
	pytest -v --cov=src --cov-report=html --cov-report=term-missing

clean:
	rm -rf data/output/*.csv
	rm -rf logs/*.log
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage
	rm -rf build dist *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

run:
	python -m src.pipeline

generate-data:
	python -m src.utils.data_generator

format:
	black src/ tests/

lint:
	flake8 src/ tests/
	mypy src/

