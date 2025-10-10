# ProfitScout — Serverless Financial Data Platform

ProfitScout is an end-to-end AI platform for financial analysis, turning raw market data into actionable investment signals for the Russell 1000. It ingests public filings and prices, enriches them with AI, and serves ranked scores and pages for downstream apps.

## Key Features

- **Broad Ingestion**: Cloud Functions pull SEC filings, fundamentals, prices, and technical indicators into Google Cloud Storage and BigQuery.
- **AI Enrichment**: Vertex AI summarizers and analyzers transform raw data into structured insights.
- **Automated Serving**: Score aggregation, recommendation generation, and site content are produced and synced to Firestore.
- **Options Analysis**: Ingests options chain data, enriches it with feature engineering, and generates daily buy/sell recommendations.
- **Workflow-First**: Cloud Workflows coordinate each stage and run on a Cloud Scheduler trigger for fully serverless operation.

## How It Works

On a schedule, a workflow fan-outs to ingestion jobs that collect raw data from external APIs and persist to Cloud Storage and BigQuery. A second workflow calls summarization and analysis functions that read that data, invoke Vertex AI, and write enriched artifacts. A final workflow aggregates scores, builds recommendation pages, and pushes data to Firestore for consumption. A parallel set of workflows handles the options data pipeline.

```mermaid
flowchart LR
  subgraph Core Pipeline
    Scheduler((Cloud Scheduler)) --> IngestWF[Workflow: src/ingestion/workflow.yaml]
    IngestWF --> IngestFns{{Ingestion Functions}}
    IngestFns --> GCS[(GCS: profit-scout-data)]
    IngestFns --> BQ[(BigQuery: profit_scout)]
    GCS --> EnrichWF[Workflow: src/enrichment/workflow.yaml]
    EnrichWF --> EnrichFns{{Summarizers & Analyzers}}
    EnrichFns --> GCS
    EnrichFns --> BQ
    BQ --> ServeWF[Workflow: src/serving/workflow.yaml]
    ServeWF --> ServeFns{{Scoring & Pages}}
    ServeFns --> Firestore[(Firestore: tickers)]
    ServeFns --> DestGCS[(GCS: profit-scout)]
  end

  subgraph Options Pipeline
    Scheduler --> OptionsIngestWF[Workflow: src/options/ingestion/workflow.yaml]
    OptionsIngestWF --> OptionsIngestFns{{Options Ingestion}}
    OptionsIngestFns --> BQ
    BQ --> OptionsEnrichWF[Workflow: src/options/enrichment/workflow.yaml]
    OptionsEnrichWF --> OptionsEnrichFns{{Options Enrichment}}
    OptionsEnrichFns --> BQ
    BQ --> OptionsServeWF[Workflow: src/options/serving/workflow.yaml]
    OptionsServeWF --> OptionsServeFns{{Options Serving}}
    OptionsServeFns --> Firestore
  end
```

## Repository Structure

The repository is organized into a `src` directory containing all application code and a `tests` directory for unit tests.

```
.
├── .github/
│   └── workflows/
│       └── ci.yml
├── src/
│   ├── ingestion/
│   ├── enrichment/
│   ├── serving/
│   ├── options/
│   └── utils/
├── tests/
│   ├── ingestion/
│   └── enrichment/
├── .gitignore
└── README.md
```

- **`src/ingestion/`**: Cloud Functions and pipelines for collecting raw data.
- **`src/enrichment/`**: AI-powered summarizers and analyzers.
- **`src/serving/`**: Aggregates scores and publishes recommendations.
- **`src/options/`**: Code for the options-related features.
- **`src/utils/`**: Helper scripts for deployment and data management.
- **`tests/`**: Unit tests for all application code.

## Quickstart

### Prerequisites
- Python 3.12+
- `gcloud` CLI, authenticated to a Google Cloud project.
- An environment variable `FMP_API_KEY` set with your Financial Modeling Prep API key.

### Setup
1.  **Create a virtual environment:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```

2.  **Install all dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

### Running Tests
To run the full suite of unit tests, use `pytest`:
```bash
pytest
```

## Configuration

Configuration for each component is managed within its respective `config.py` file.

### Core Configuration
| Variable                  | Default             | Description                               | Location                            |
| ------------------------- | ------------------- | ----------------------------------------- | ----------------------------------- |
| `PROJECT_ID`              | `profitscout-lx6bb` | Source Google Cloud project               | `src/ingestion/core/config.py`      |
| `GCS_BUCKET_NAME`         | `profit-scout-data` | Raw data bucket                           | `src/ingestion/core/config.py`      |
| `BIGQUERY_DATASET`        | `profit_scout`      | Dataset for ingestion outputs             | `src/ingestion/core/config.py`      |
| `FMP_API_KEY_SECRET`      | `FMP_API_KEY`       | Secret name for FMP API key               | `src/ingestion/core/config.py`      |
| `SEC_API_KEY_SECRET`      | `SEC_API_KEY`       | Secret name for SEC API key               | `src/ingestion/core/config.py`      |
| `MODEL_NAME`              | `gemini-2.0-flash`  | Vertex model for summaries                | `src/enrichment/core/config.py`     |
| `DESTINATION_PROJECT_ID`  | `profitscout-fida8` | Target project for serving assets         | `src/serving/core/config.py`        |
| `DESTINATION_GCS_BUCKET`  | `profit-scout`      | Bucket for public artifacts               | `src/serving/core/config.py`        |
| `FIRESTORE_COLLECTION`    | `tickers`           | Firestore collection for serving          | `src/serving/core/config.py`        |

### Options Configuration
| Variable                  | Default             | Description                               | Location                            |
| ------------------------- | ------------------- | ----------------------------------------- | ----------------------------------- |
| `POLYGON_API_KEY`         | `None`              | Polygon API key                           | `src/options/ingestion/core/config.py` |
| `OPTIONS_CHAIN_TABLE`     | `options_chain`     | BigQuery table for options chain data     | `src/options/ingestion/core/config.py` |
| `SCORES_TABLE`            | `analysis_scores`   | BigQuery table for analysis scores        | `src/options/enrichment/core/config.py`|
| `CAND_TABLE`              | `options_candidates`| BigQuery table for options candidates     | `src/options/enrichment/core/config.py`|
| `PRICE_TABLE_ID`          | `price_data`        | BigQuery table for price data             | `src/options/enrichment/core/config.py`|
| `OPTIONS_MD_PREFIX`       | `options-recommendations/` | GCS prefix for options recommendations | `src/options/serving/core/config.py`   |

## CI/CD
This repository uses GitHub Actions for continuous integration. The workflow, defined in `.github/workflows/ci.yml`, runs automatically on every push and pull request to:
1.  Install all dependencies.
2.  Run the `black` formatter to check for code style.
3.  Execute the `pytest` suite to ensure code quality and correctness.

## Contributing

1.  Create a feature branch off `main`.
2.  Make your changes and ensure they are well-documented.
3.  Format your code with `black .` and run tests with `pytest` before committing.
4.  Submit a pull request describing the change and referencing any related issues.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.