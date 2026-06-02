.PHONY: up down logs test lint format

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
