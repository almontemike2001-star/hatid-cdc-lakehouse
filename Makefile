.PHONY: up down reset setup demo simulate migrate-tips reconcile transform report dagster test test-unit lint bi-up dashboards screenshots

up:            ## start Postgres, Redpanda, Debezium and ClickHouse
	docker compose up -d --wait

down:
	docker compose down

reset:         ## stop and delete all data (Postgres, ClickHouse, Kafka offsets)
	docker compose --profile bi down -v

setup:         ## seed the app DB, create ClickHouse bronze, register the Debezium connector
	uv run hatid setup

demo:          ## setup + 3 simulated days with a live schema change + dbt + report
	uv run hatid demo

simulate:      ## advance the simulated app by 6 hours
	uv run hatid simulate --hours 6

migrate-tips:  ## live schema change: add orders.tip_amount (ClickHouse, then Postgres)
	uv run hatid migrate-tips

reconcile:     ## compare Postgres and ClickHouse table by table
	uv run hatid reconcile

transform:     ## dbt build on ClickHouse
	uv run hatid transform

report:        ## render the KPI report and copy it to docs/ for GitHub Pages
	uv run hatid report --out docs/kpi_report.html

dagster:       ## Dagster UI on http://localhost:3003
	DAGSTER_HOME="$(CURDIR)/.dagster_home" uv run dagster dev -p 3003

test:          ## unit tests + stack tests (stack tests need `make up setup`)
	uv run pytest -v

test-unit:
	uv run pytest -v -m "not stack"

lint:
	uv run ruff check . && uv run ruff format --check .

bi-up:         ## start Metabase on http://localhost:3002
	docker compose --profile bi up -d --wait

dashboards:    ## build the Metabase dashboards through the API
	uv run python bi/metabase/build_dashboards.py

screenshots:   ## capture the public dashboards into docs/images
	cd bi/metabase && npm install --silent
	node bi/metabase/screenshot.mjs bi/metabase/dashboards.json docs/images
