# update_forecast-main

Forecasting utility using Facebook Prophet to generate time-series predictions and load results into BigQuery. Credentials are supplied via environment variables; no secrets are stored in the repo.

## Key files
- `update_forecast.py`: builds and runs the Prophet model, writes forecasts.
- `Dockerfile`: containerizes the job for deployment.

## Configure (env vars)
- `GOOGLE_APPLICATION_CREDENTIALS` or `SERVICE_ACCOUNT_FILE` for BigQuery auth.
- `GCP_PROJECT_ID`, `BQ_DATASET`, `BQ_TABLE` for output destinations.
- Any date ranges/filters you wire into the script.

## Run locally
```bash
pip install -r requirements.txt  # if present
python update_forecast.py
```

## Notes
- Use mock/non-sensitive data for demos.
- Add monitoring/retries before production use.***
