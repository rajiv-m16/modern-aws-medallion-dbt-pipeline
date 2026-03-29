import os
import json
import logging
from datetime import datetime

import requests
from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.cloud import storage

app = Flask(__name__)

# -------------------------
# CONFIGURATION
# -------------------------
BQ_LOCATION = "asia-southeast1"

DATASET_NAME = os.getenv("DATASET_NAME")
BUCKET_NAME = os.getenv("BUCKET_NAME")
API_BASE_URL = os.getenv("API_BASE_URL", "https://dummyjson.com")
LIMIT = int(os.getenv("LIMIT", "10"))
REQUEST_TIMEOUT = 20

if not DATASET_NAME or not BUCKET_NAME:
    raise ValueError("DATASET_NAME and BUCKET_NAME must be set")

bq_client = bigquery.Client()
storage_client = storage.Client()

logging.basicConfig(level=logging.INFO)


# -------------------------
# Get Last Processed Offset
# -------------------------
def get_offset(entity_name):
    query = f"""
    SELECT last_processed_offset
    FROM `{DATASET_NAME}.pipeline_metadata`
    WHERE entity_name = @entity_name
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "entity_name", "STRING", entity_name
            )
        ]
    )

    result = bq_client.query(
        query,
        job_config=job_config,
        location=BQ_LOCATION
    ).result()

    for row in result:
        return row.last_processed_offset

    return 0


# -------------------------
# Save Raw Data to GCS
# -------------------------
def save_to_gcs(entity_name, offset, records):

    bucket = storage_client.bucket(BUCKET_NAME)
    today = datetime.utcnow()

    path = (
        f"{entity_name}/"
        f"{today.year}/{today.month}/{today.day}/"
        f"{entity_name}_{offset}.json"
    )

    blob = bucket.blob(path)

    ndjson_data = "\n".join(json.dumps(record) for record in records)

    blob.upload_from_string(
        ndjson_data,
        content_type="application/json"
    )

    logging.info(f"Saved to gs://{BUCKET_NAME}/{path}")

    return f"gs://{BUCKET_NAME}/{path}"


# -------------------------
# Main Endpoint
# -------------------------
@app.route("/", methods=["POST"])
def run_pipeline():

    data = request.get_json()

    if not data or "entity_name" not in data:
        return jsonify({
            "status": "FAILED",
            "error": "entity_name missing"
        }), 400

    entity_name = data["entity_name"]

    try:
        # 1️ Get Offset
        offset = get_offset(entity_name)

        # 2️ Call API
        url = f"{API_BASE_URL}/{entity_name}?limit={LIMIT}&skip={offset}"

        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        api_data = response.json()
        records = api_data.get(entity_name, [])

        if not records:
            return jsonify({
                "status": "NO_DATA",
                "offset_used": offset
            })

        # 3️ Add extraction timestamp
        extraction_time = datetime.utcnow().isoformat()

        for record in records:
            record["extraction_timestamp"] = extraction_time
            record["entity_name"] = entity_name

        # 4️ Save to GCS
        gcs_uri = save_to_gcs(entity_name, offset, records)

        # 5️ Return structured response for Workflow
        return jsonify({
            "status": "INGESTION_SUCCESS",
            "entity_name": entity_name,
            "gcs_uri": gcs_uri,
            "offset_used": offset,
            "records_fetched": len(records),
            "new_offset": offset + LIMIT
        })

    except Exception as e:
        logging.error(str(e))

        return jsonify({
            "status": "FAILED",
            "error": str(e)
        }), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
