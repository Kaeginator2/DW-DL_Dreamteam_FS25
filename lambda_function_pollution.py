import os
import json
import boto3
import requests
import pandas as pd
from datetime import datetime
from io import StringIO
import time

# Initialize S3 client
s3 = boto3.client("s3")

def lambda_handler(event, context):

    source_bucket = os.environ['SOURCE_BUCKET']
    source_key = os.environ['SOURCE_KEY']
    target_bucket = os.environ['TARGET_BUCKET']

    # API key and endpoint
    API_KEY = os.environ['API_KEY']
    API_URL = "http://api.openweathermap.org/data/2.5/air_pollution"

    response = s3.get_object(Bucket=source_bucket, Key=source_key)
    csv_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))

    df_api = df[["name", "latitude_deg", "longitude_deg"]]

    now = datetime.utcnow()

    s3_key = f"pollution_data_raw/raw/year={now.year}/month={now.month:02}/day={now.day:02}/hour={now.hour:02}/data.jsonl"

    tic = time.time()
    records = []

    for _, row in df_api.iterrows():
        lat = row["latitude_deg"]
        lon = row["longitude_deg"]

        try:
            response = requests.get(
                API_URL,
                params={
                    "lat": lat,
                    "lon": lon,
                    "appid": API_KEY
                },
                timeout=10
            )

            if response.status_code == 200:
                record = response.json()
                record["metadata"] = {
                    "name": row["name"],
                    "lat": lat,
                    "lon": lon
                }
                records.append(record)
            else:
                print(f"API error for ({lat}, {lon}): {response.status_code}")

        except Exception as e:
            print(f"Exception for ({lat}, {lon}): {str(e)}")

    buffer = StringIO()
    for record in records:
        json_line = json.dumps(record)
        buffer.write(json_line + "\n")
    
    buffer.seek(0)

    s3.put_object(
        Bucket=target_bucket,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/json"
    )

    elapsed_time = time.time() - tic

    return {
        "statusCode": 200,
        "body": f"{len(records)} records uploaded to s3://{target_bucket}/{s3_key}",
        "time_used": elapsed_time
    }
