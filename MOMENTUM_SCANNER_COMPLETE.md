# Momentum Scanner Task Completion

## Status
- ✅ `src/ingestion/core/clients/polygon_client.py`: Updated with `fetch_option_contract_snapshot` and `fetch_stock_snapshot`.
- ✅ `src/enrichment/core/pipelines/momentum_scanner.py`: Created with main logic.
- ✅ `src/enrichment/main.py`: Updated to expose `run_momentum_scanner`.
- ✅ `workflows/momentum_workflow.yaml`: Created.

## Files Changed
- `src/ingestion/core/clients/polygon_client.py`
- `src/enrichment/core/pipelines/momentum_scanner.py`
- `src/enrichment/main.py`
- `workflows/momentum_workflow.yaml`

## Deployment Commands

### Cloud Function
**Note:** This function imports `src.ingestion.core.clients.polygon_client`. To ensure the import works, deploy from the project root so the `src` package is preserved in the structure.

```bash
gcloud functions deploy momentum-scanner \
    --gen2 \
    --runtime=python312 \
    --region=us-central1 \
    --source=. \
    --entry-point=src.enrichment.main.run_momentum_scanner \
    --trigger-http \
    --allow-unauthenticated \
    --memory=512MiB \
    --timeout=600
```

### Workflow

```bash
gcloud workflows deploy momentum-workflow \
    --source=workflows/momentum_workflow.yaml \
    --location=us-central1
```

## Notes
- The `momentum-scanner` logic relies on `PolygonClient` from the ingestion module. Unlike other enrichment functions which are self-contained in `src/enrichment`, this one crosses module boundaries. The deployment command above uses `--source=.` to handle this.
- The workflow is set to a 10-minute timeout (`timeout: 600`) which is well within the expected execution time.
