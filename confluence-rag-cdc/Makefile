.PHONY: test build-aws deploy-aws deploy-dbx

test:
	pip install -q --break-system-packages pytest boto3 beautifulsoup4 langchain-text-splitters && PYTHONPATH=src:src/lambdas/ingest pytest -q tests

build-aws:
	sam build -t infra/template.yaml

deploy-aws: build-aws
	sam deploy --guided

deploy-dbx:
	databricks bundle deploy -t dev

run-dbx:
	databricks bundle run -t dev confluence_rag_job
