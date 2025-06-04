import os
import json
import boto3
import requests
import pandas as pd
from datetime import datetime
from io import StringIO
import time

# Create S3 client
s3 = boto3.client("s3")

def lambda_handler(event, context):
    # S3 client
    s3_flightdata = boto3.client("s3")
    
    # Load environment variables for sourcebucket
    source_bucket = os.environ['SOURCE_BUCKET']
    source_key = os.environ['SOURCE_KEY']

    # read flight csv
    response = s3_flightdata.get_object(Bucket=source_bucket, Key=source_key)
    csv_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))

    # get the important Data from csv
    df_api = df[["name", "latitude_deg", "longitude_deg"]]
    
    # Load environment variables for datalake
    API_KEY = os.getenv("API_KEY")
    TARGET_BUCKET = os.getenv("TARGET_BUCKET")

    if not API_KEY or not TARGET_BUCKET:
        raise ValueError("Missing environment variables: API_KEY or TARGET_BUCKET")

    # Use only the first 5 rows for testing
    #df_api = df_api.head(100)

    # Get current UTC time for S3 path partitioning
    now = datetime.utcnow()
    s3_key = f"weatherdata_raw/raw/year={now.year}/month={now.month:02}/day={now.day:02}/hour={now.hour:02}/data.jsonl"

    tic = time.time()
    records = []

    for _, row in df_api.iterrows():
        lat = row["latitude_deg"]
        lon = row["longitude_deg"]

        try:
            # Send request to weather API using lat/lon
            response = requests.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params={
                    "lat": lat,
                    "lon": lon,
                    "appid": API_KEY,
                    "units": "metric"
                },
                timeout=10
            )

            # Append result if successful
            if response.status_code == 200:
                records.append(response.json())
            else:
                print(f"API error for ({lat}, {lon}): {response.status_code}")

        except Exception as e:
            print(f"Exception for ({lat}, {lon}): {str(e)}")

    # Prepare JSON Lines content
    buffer = StringIO()
    for record in records:
        json_line = json.dumps(record)
        buffer.write(json_line + "\n")

    buffer.seek(0)

    # Upload JSONL to S3
    s3.put_object(
        Bucket=TARGET_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/json"
    )
    elapsed_time = time.time() - tic
    return {
        "statusCode": 200,
        "body": f"{len(records)} records uploaded to s3://{TARGET_BUCKET}/{s3_key}",
        "time_used": elapsed_time
    }
    
