project_id='watchdog-340107'

gcloud docker -- build -t gcr.io/$project_id/update-forecast .

gcloud docker -- push gcr.io/$project_id/update-forecast

gcloud run deploy update-forecast --image gcr.io/$project_id/update-forecast --service-account $project_id@appspot.gserviceaccount.com --memory 4Gi --region us-central1 --platform managed --allow-unauthenticated --timeout=20m

#endpoint='https://update-forecast-l5dva4wyha-uc.a.run.app'
#gcloud pubsub topics create update_forecast
#gcloud pubsub subscriptions create update_forecast_cloud_run  --push-endpoint=$endpoint --topic=update_forecast --ack-deadline=600 --message-retention-duration=600
