.PHONY: run data ingest transform stream ml eda test clean download-real validate

run:        ## Run the full end-to-end pipeline (synthetic sample)
	bash run.sh

data:       ## Generate the sample dataset only
	python3 data/generate_sample.py

download-real: ## Download a real TLC monthly file into data/real/ (override: M=2024-03)
	python3 data/download_tlc.py $(M)

validate:   ## Run the pipeline against the downloaded real TLC file (M=2024-01)
	TAXI_SOURCE=data/real/yellow_tripdata_$(or $(M),2024-01).parquet \
	TAXI_ZONES=data/real/taxi_zone_lookup.csv \
	bash run.sh

ingest:     ## Ingestion stage only
	python3 src/ingestion.py

transform:  ## Transformations + Spark SQL only
	python3 src/transformations.py

stream:     ## Structured Streaming stage only
	python3 src/streaming.py

ml:         ## MLlib regression stage only
	python3 src/ml_pipeline.py

eda:        ## Exploratory data analysis only
	python3 src/eda.py

test:       ## Run the unit test suite
	python3 -m pytest -q

clean:      ## Remove generated data and outputs
	rm -rf data/curated data/outputs data/stream_source data/_checkpoints
	find . -name "__pycache__" -type d -prune -exec rm -rf {} +
