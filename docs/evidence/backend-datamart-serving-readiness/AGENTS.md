<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-28T00:00:00Z -->

# docs/evidence/backend-datamart-serving-readiness

## Purpose
Release evidence path for the claim that the backend-facing data marts are ready to serve. Automated
PR checks cover the local Docker PostgreSQL aggregate path with a deterministic fixture; live object
storage proof and live crawl results are recorded here by hand because they depend on external store
responses and credentials that are out of scope for CI.

## Key Files

| File | Description |
|------|-------------|
| `README.md` | States the automated-vs-manual proof boundary and what a release must record here |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- Record only what actually ran: command, target service, timestamp, `target_date`, row counts for
  all four mart tables, and any failures or caveats. Never synthesize numbers.
- The four tables in scope are `fact_service_review_daily`, `fact_service_aspect_daily`,
  `fact_category_radar_scores`, and `srv_daily_review_list`.
- The capture procedure and the SQL shape for row counts are in
  `docs/local-development.md` ("Manual live crawl smoke evidence") and
  `docs/backend-datamart-serving-readiness.md`.

### Testing Requirements
The PR-safe portion of the proof:

```bash
docker compose -f docker-compose.test.yml up -d test-postgres
TEST_DATABASE_URL="${TEST_DATABASE_URL:-postgresql://testuser:testpass@localhost:5433/testdb}" \
  PYTHONPATH=. uv run pytest tests/test_backend_datamart_contract.py tests/test_backend_datamart_serving_readiness.py -q
```
The manual portion uses the real entrypoint:
`PYTHONPATH=. uv run python scripts/run_pipeline.py --steps gold,aggregate --target-date YYYY-MM-DD`.

### Common Patterns
Evidence entries are dated and paired with the exact command that produced them, so a reviewer can
re-run the same thing.

## Dependencies

### Internal
`docs/backend-datamart-serving-readiness.md`, `docs/backend-datamart-contract.md`,
`src/gold/aggregator.py`, `src/pipeline/post_aggregate_validation.py`, `docker-compose.test.yml`.

### External
Live App Store / Play Store responses and a real `OPENAI_API_KEY` for the manual portion.

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
