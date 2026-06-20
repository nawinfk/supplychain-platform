PYTHON ?= python3.12
VENV ?= .venv
PY := $(VENV)/bin/python

.PHONY: setup install clean test lint fmt db-up db-down db-reset db-logs db-shell db-wait db-init run-ais-producer run-vessel-worker run-api local-validate docker-build k8s-namespace k8s-secrets-template deploy-local

setup:
	test -x $(PY) || $(PYTHON) -m venv $(VENV)
	$(PY) -m pip install --upgrade pip
	$(MAKE) install PYTHON=$(PY)

install:
	$(PYTHON) -m pip install -e '.[dev]'
	rm -rf supplychain_platform.egg-info

clean:
	find . -path './$(VENV)' -prune -o -type d -name '__pycache__' -exec rm -rf {} +
	find . -type f -name '*.pyc' ! -path './$(VENV)/*' -delete
	find . -path './$(VENV)' -prune -o -type d -name '*.egg-info' -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache build dist

test:
	$(PY) -m pytest --cov=libs --cov-report=term-missing

lint:
	$(PY) -m ruff check .

fmt:
	$(PY) -m black .
	$(PY) -m ruff format .

db-up:
	docker compose up -d postgres

db-down:
	docker compose down

db-reset:
	docker compose down -v
	docker compose up -d postgres

db-logs:
	docker logs -f supplychain-postgres

db-shell:
	docker compose exec postgres psql -U supplychain -d supplychain

db-wait:
	@until docker compose exec -T postgres pg_isready -U supplychain -d supplychain >/dev/null 2>&1; do sleep 1; done

db-init:
	docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U supplychain -d supplychain < database/schema.sql

run-ais-producer:
	PYTHONPATH=. ENV=local AIS_MOCK_MODE=true KAFKA_API_KEY=dummy KAFKA_API_SECRET=dummy $(PY) services/ais-producer/main.py

run-vessel-worker:
	PYTHONPATH=. $(PY) services/vessel-consumer-worker/main.py

run-api:
	PYTHONPATH=. $(PY) services/api-service/main.py

local-validate:
	$(MAKE) clean
	$(MAKE) setup
	$(MAKE) db-up
	$(MAKE) db-wait
	$(MAKE) db-init
	$(MAKE) test

docker-build:
	@for service in ais-producer weather-producer news-producer vessel-consumer-worker risk-worker api-service; do \
		docker build -f services/$$service/Dockerfile -t supplychain/$$service:local . || exit 1; \
	done

k8s-namespace:
	kubectl apply -f deploy/k8s/namespace.yaml

k8s-secrets-template:
	@echo "Copy deploy/k8s/secret-template.yaml, replace placeholders, and apply the private copy."

deploy-local:
	kubectl apply -k deploy/k8s
