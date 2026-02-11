# PROMPT: Deploy & Test Momentum Scanner

## Context
The momentum scanner code is written in `src/enrichment/core/pipelines/momentum_scanner.py` and exposed via `src/enrichment/main.py` as `run_momentum_scanner`. 

**CRITICAL ISSUE:** The existing enrichment functions deploy with `--source=src/enrichment`. But `momentum_scanner.py` imports from `src.ingestion.core.clients.polygon_client` — this cross-module import will FAIL when deployed from `src/enrichment` because `src/ingestion` won't be in the deployment package.

## Tasks

### Task 1: Fix the Cross-Module Import
Options (pick the best one):
1. **Copy the two Polygon snapshot methods** into a local helper in `src/enrichment/core/clients/` so there's no cross-module dependency. The methods needed are `fetch_stock_snapshot` and `fetch_option_contract_snapshot` from `src/ingestion/core/clients/polygon_client.py`. You also need the Polygon API key — check how other enrichment pipelines access config (look at `src/enrichment/core/config.py` for `POLYGON_API_KEY` or add it).
2. OR deploy from project root with adjusted entry point — but this is messy, don't do this.

**Go with option 1.** Create `src/enrichment/core/clients/polygon_client.py` with just the snapshot methods and a simple HTTP client (requests). Update `momentum_scanner.py` imports accordingly.

### Task 2: Deploy the Cloud Function
```bash
gcloud functions deploy run_momentum_scanner \
    --gen2 \
    --region=us-central1 \
    --runtime=python312 \
    --source=src/enrichment \
    --entry-point=run_momentum_scanner \
    --trigger-http \
    --allow-unauthenticated \
    --timeout=540s \
    --memory=2GiB \
    --cpu=1
```

Note: Entry point naming convention matches existing functions (e.g., `run_options_analyzer`, `run_options_candidate_selector`). Use `run_momentum_scanner` not `momentum-scanner`.

### Task 3: Test the Function
After deployment, invoke it:
```bash
curl -X POST "https://us-central1-profitscout-lx6bb.cloudfunctions.net/run_momentum_scanner"
```

Check logs:
```bash
gcloud functions logs read run_momentum_scanner --region=us-central1 --limit=50
```

If there are errors, fix them and redeploy. Iterate until it runs successfully.

**NOTE:** The function may return empty results if the morning pipeline hasn't run today — that's OK. We just need it to execute without errors (connect to BigQuery, query candidates, attempt Polygon calls).

### Task 4: Deploy the Workflow
```bash
gcloud workflows deploy momentum-workflow \
    --source=workflows/momentum_workflow.yaml \
    --location=us-central1 \
    --project=profitscout-lx6bb
```

Update `workflows/momentum_workflow.yaml` if needed — make sure the function URL matches the deployed function name (`run_momentum_scanner` not `momentum-scanner`).

### Task 5: Add to CD Pipeline
Add `run_momentum_scanner` to the FUNCTIONS array in `.github/workflows/cd.yml` under `deploy-enrichment` so it auto-deploys on future pushes.

## Environment
- Project: `profitscout-lx6bb`
- Region: `us-central1`
- Runtime: `python312`
- Enrichment source: `src/enrichment`
- Existing enrichment config: `src/enrichment/core/config.py`

## Completion Marker
When ALL tasks are done, write `DEPLOY_MOMENTUM_COMPLETE.md` to the ROOT of this repo with:
- ✅/❌ status for each task
- Function URL
- Test output or error logs
- Any files created/modified
- If any errors remain unresolved, document them clearly
