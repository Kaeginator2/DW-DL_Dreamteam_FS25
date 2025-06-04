import csv
import io
import json
import datetime
import logging
from datetime import timezone
import os

import boto3
import requests

# ─── Configuration ──────────────────────────────────────────────────────────────
API_KEY            =    os.environ['API_Key']
S3_BUCKET          =    os.environ['Lake_Name']
CSV_BATCH_PREFIX   = 'batches'              # S3 folder where airports_batch_X.csv reside
DAY_OFFSET         = '-1'                    # '-1' = yesterday
S3_OUTPUT_PREFIX   = 'flightdata'            # base prefix for outputs
BATCH_COUNT        = 10                      # total number of split CSVs now
# ────────────────────────────────────────────────────────────────────────────────

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client     = boto3.client('s3')
lambda_client = boto3.client('lambda')

def load_airports(batch_id):
    """
    Load the semicolon‑delimited CSV for this batch_id from S3,
    expecting filenames like airports_batch_1.csv … airports_batch_10.csv.
    """
    key = f'{CSV_BATCH_PREFIX}/airports_batch_{batch_id}.csv'
    resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    text = resp['Body'].read().decode('utf-8')
    reader = csv.DictReader(io.StringIO(text), delimiter=';')
    # assume each split CSV already contains only large_airport rows
    return [row['iata_code'].strip() for row in reader if row.get('iata_code')]

def fetch_departures(iata):
    """
    One‑shot call to FlightAPI schedule endpoint for departures.
    """
    url    = f'https://api.flightapi.io/schedule/{API_KEY}'
    params = {'mode':'departures', 'iata':iata, 'day':DAY_OFFSET}
    r      = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def lambda_handler(event, context):
    # Determine batch
    batch_id = int(event.get('batch_id', 1))

    # Log start
    start_time = datetime.datetime.now(timezone.utc)
    logger.info(f"Starting batch {batch_id}/{BATCH_COUNT} at {start_time.isoformat()}")

    # Prepare keys
    date_str      = start_time.strftime('%Y%m%d')
    date_prefix   = f'{S3_OUTPUT_PREFIX}/{date_str}'
    partial_key   = f'{date_prefix}/batch{batch_id}.json'
    aggregate_key = f'{S3_OUTPUT_PREFIX}/{date_str}.json'

    # Load and process this batch
    airports = load_airports(batch_id)
    if not airports:
        logger.error(f"No airports found in batch {batch_id}")
        return {'statusCode':400, 'body':json.dumps({'error':f'No airports in batch {batch_id}'})}

    results = {}
    for iata in airports:
        try:
            results[iata] = fetch_departures(iata)
        except Exception as e:
            logger.warning(f"Error fetching {iata}: {e}")
            results[iata] = {'error': str(e)}

    # Write batch JSON
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=partial_key,
        Body=json.dumps(results),
        ContentType='application/json'
    )
    logger.info(f"Wrote partial results to s3://{S3_BUCKET}/{partial_key}")

    # Chain or aggregate
    if batch_id < BATCH_COUNT:
        # Invoke next batch asynchronously
        lambda_client.invoke(
            FunctionName=context.function_name,
            InvocationType='Event',
            Payload=json.dumps({'batch_id': batch_id + 1})
        )
        logger.info(f"Invoked next batch: {batch_id + 1}")
    else:
        # Final batch: aggregate all partials
        all_data = {}
        for bid in range(1, BATCH_COUNT + 1):
            pk  = f'{date_prefix}/batch{bid}.json'
            obj = s3_client.get_object(Bucket=S3_BUCKET, Key=pk)
            part = json.loads(obj['Body'].read())
            all_data.update(part)
        # Write aggregated JSON to 'grouped' subfolder
        aggregate_key = f'{S3_OUTPUT_PREFIX}/grouped/{date_str}.json'

        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=aggregate_key,
            Body=json.dumps(all_data),
            ContentType='application/json'
        )
        logger.info(f"Wrote aggregated results to s3://{S3_BUCKET}/{aggregate_key}")

    # Log end and duration
    end_time = datetime.datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    logger.info(f"Finished batch {batch_id} in {duration:.2f} seconds")

    return {
        'statusCode': 200,
        'body': json.dumps({
            'batch_id':     batch_id,
            'partial_key':  partial_key,
            **({'next_batch_id': batch_id + 1} if batch_id < BATCH_COUNT else {'aggregate_key': aggregate_key}),
            'duration_s':   duration
        })
    }


