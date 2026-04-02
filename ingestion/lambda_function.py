import os
import json
import logging
from datetime import datetime

import requests
import boto3
from botocore.exceptions import ClientError

# -------------------------
# CONFIGURATION
# -------------------------
# These are set in the AWS Lambda Environment Variables
S3_BUCKET = os.getenv("S3_BUCKET")
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "pipeline_metadata")
API_BASE_URL = os.getenv("API_BASE_URL", "https://dummyjson.com")
LIMIT = int(os.getenv("LIMIT", "10"))
REQUEST_TIMEOUT = 20

# Initialize AWS Clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# -------------------------
# Get Last Processed Offset (DynamoDB)
# -------------------------
def get_offset(entity_name):
    table = dynamodb.Table(DYNAMODB_TABLE)
    try:
        response = table.get_item(Key={'entity_name': entity_name})
        if 'Item' in response:
            return int(response['Item'].get('last_processed_offset', 0))
    except ClientError as e:
        logger.error(f"Error fetching offset: {e.response['Error']['Message']}")
    
    return 0

# -------------------------
# Save Raw Data to S3
# -------------------------
def save_to_s3(entity_name, offset, records):
    today = datetime.utcnow()
    # Path mirrors your previous GCS structure for consistency
    path = (
        f"{entity_name}/"
        f"{today.year}/{today.month}/{today.day}/"
        f"{entity_name}_{offset}.json"
    )

    # Convert records to Newline Delimited JSON (ndjson)
    ndjson_data = "\n".join(json.dumps(record) for record in records)

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=path,
            Body=ndjson_data,
            ContentType="application/json"
        )
        logger.info(f"Saved to s3://{S3_BUCKET}/{path}")
        return f"s3://{S3_BUCKET}/{path}"
    except ClientError as e:
        logger.error(f"Error uploading to S3: {e.response['Error']['Message']}")
        raise

# -------------------------
# Lambda Handler (Main Entry)
# -------------------------
def lambda_handler(event, context):
    """
    AWS Lambda entry point. 
    Accepts JSON input like: {"entity_name": "products"}
    """
    entity_name = event.get("entity_name")

    if not entity_name:
        return {
            "status": "FAILED",
            "error": "entity_name missing in input event"
        }

    try:
        # 1️ Get Offset from DynamoDB
        offset = get_offset(entity_name)

        # 2️ Call API
        url = f"{API_BASE_URL}/{entity_name}?limit={LIMIT}&skip={offset}"
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        api_data = response.json()
        records = api_data.get(entity_name, [])

        if not records:
            return {
                "status": "NO_DATA",
                "offset_used": offset
            }

        # 3️ Add extraction timestamp
        extraction_time = datetime.utcnow().isoformat()
        for record in records:
            record["extraction_timestamp"] = extraction_time
            record["entity_name"] = entity_name

        # 4️ Save to S3 (Replaces GCS)
        s3_uri = save_to_s3(entity_name, offset, records)

        # 5️ Update Offset in DynamoDB (Replaces BigQuery Update)
        new_offset = offset + len(records)
        table = dynamodb.Table(DYNAMODB_TABLE)
        table.put_item(
            Item={
                'entity_name': entity_name,
                'last_processed_offset': new_offset,
                'last_run_timestamp': extraction_time,
                'status': 'SUCCESS'
            }
        )

        # 6️ Return structured response for Step Functions
        return {
            "status": "INGESTION_SUCCESS",
            "entity_name": entity_name,
            "s3_uri": s3_uri,
            "offset_used": offset,
            "records_fetched": len(records),
            "new_offset": new_offset
        }

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        return {
            "status": "FAILED",
            "error": str(e)
        }