import requests
import os
from datetime import datetime, timedelta
from google.cloud import bigquery
from flask import Flask

app = Flask(__name__) 

POSTHOG_API_KEY = 'phc_5d8L6f1i7r2a1hSg21c1qqbnGAKNnB5I8LSfG33y2aB'
POSTHOG_URL = 'https://app.posthog.com'
BQ_PROJECT = 'petproject-475319' 
BQ_DATASET = 'posthog_data'
BQ_TABLE = 'events'


@app.route("/", methods=["GET"]) 
def fetch_and_load():
    """Головна функція, яку буде викликати Cloud Scheduler."""
    
    since_time = (datetime.utcnow() - timedelta(minutes=16)).isoformat()
    

    api_url = f"{POSTHOG_URL}/api/projects/2/events?after={since_time}"
    headers = {'Authorization': f'Bearer {POSTHOG_API_KEY}'}
    
    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        events = response.json().get('results', [])
    except requests.RequestException as e:
        print(f"Error fetching from PostHog: {e}")
        return (f"PostHog Error: {e}", 500)

    if not events:
        print("No new events found.")
        return ("No new events", 200)

    rows_to_insert = []
    for event in events:
        rows_to_insert.append({
            "event_id": event.get('uuid'),
            "event_name": event.get('event'),
            "distinct_id": event.get('distinct_id'),
            "event_timestamp": event.get('timestamp'),
            "properties": event.get('properties', {})
        })

    client = bigquery.Client(project=BQ_PROJECT)
    table_ref = client.dataset(BQ_DATASET).table(BQ_TABLE)
    
    errors = client.insert_rows_json(table_ref, rows_to_insert, row_ids=[row['event_id'] for row in rows_to_insert])
    
    if not errors:
        print(f"Successfully inserted {len(rows_to_insert)} rows.")
        return (f"OK: Inserted {len(rows_to_insert)} rows", 200)
    else:
        print(f"Errors encountered while inserting rows: {errors}")
        return (f"BigQuery Error: {errors}", 500)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))