.PHONY: install test integration sanity-ingestion lint fmt check-uniswap-coverage full-run full-run-month analyze-full-run

install:
	poetry install

test:
	poetry run pytest -q -m "not integration"

integration:
	poetry run pytest -q -m integration

sanity-ingestion:
	poetry run pytest -q -rs -m integration tests/integration/test_raw_ingestion_sanity.py

lint:
	poetry run ruff check .

fmt:
	poetry run ruff format .

check-uniswap-coverage:
	poetry run python scripts/check_uniswap_subgraph_vs_chain.py $(ARGS)

full-run:
	poetry run ingestion-cli full-run $(ARGS)

full-run-month:
	poetry run ingestion-cli full-run \
		--start-time-utc 2024-06-01T00:00:00Z \
		--end-time-utc 2024-07-01T00:00:00Z \
		--rpc-mode feehistory \
		--rpc-feehistory-blocks-per-request 1000 \
		--rpc-progress-every-blocks 5000 \
		$(ARGS)

analyze-full-run:
	poetry run python scripts/analyze_full_run.py $(ARGS)
