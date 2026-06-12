.PHONY: up down logs test lint format dashboard dbt-run dbt-test dbt-docs

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f kafka

test:
	pytest tests/ -v

lint:
	flake8 producer/ consumer/ stream_processor/ storage/ tests/ --max-line-length=100

format:
	black producer/ consumer/ stream_processor/ storage/ tests/
	isort producer/ consumer/ stream_processor/ storage/ tests/

dashboard:
	streamlit run dashboard/app.py --server.port 8501

dbt-run:
	cd dbt_project && dbt run

dbt-test:
	cd dbt_project && dbt test

dbt-docs:
	cd dbt_project && dbt docs generate && dbt docs serve
