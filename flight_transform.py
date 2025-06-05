import json
import csv
import boto3
from datetime import datetime, timezone
from io import StringIO

s3 = boto3.client("s3")

# === CONFIG ===
BUCKET = environ.os['Bucket_name']
AIRPORT_LOOKUP_KEY = "Airport_list/airports.csv"
MASTER_CSV_KEY = "flightdata/processed/master_flights.csv"
DAILY_JSON_PREFIX = "flightdata/grouped/"

# === HELPERS ===

def to_datetime(ts):
    if ts:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')
    return ""

def load_airport_lookup_from_string(csv_text):
    airport_lookup = {}
    reader = csv.DictReader(StringIO(csv_text), delimiter=",", quotechar='"')
    for row in reader:
        code = row.get("iata_code", "").strip()
        name = row.get("name", "").strip()
        if code:
            airport_lookup[code] = name
    return airport_lookup

def extract_flight_data(flight, airport_code, airport_name, delay_arr, delay_dep):
    try:
        flight_number = flight["identification"]["number"]["default"]
        scheduled = flight["time"]["scheduled"]["departure"]
        real = flight["time"]["real"]["departure"] or flight["time"]["estimated"]["departure"]
    except (KeyError, TypeError):
        return None
    if not real:
        return None
    delay_minutes = round((real - scheduled) / 60)
    return {
        "airport_code": airport_code,
        "airport_name": airport_name,
        "delay_index_arrivals": delay_arr,
        "delay_index_departures": delay_dep,
        "flight_number": flight_number,
        "scheduled_departure": to_datetime(scheduled),
        "real_departure": to_datetime(real),
        "delay_minutes": delay_minutes
    }

def process_json(data, airport_lookup, flight_day):
    records = []
    for airport_code, airport_data in data.items():
        if airport_code not in airport_lookup:
            continue
        airport_name = airport_lookup[airport_code]
        delay_index = airport_data.get("airport", {}).get("pluginData", {}).get("details", {}).get("delayIndex", {})
        delay_arr = delay_index.get("arrivals")
        delay_dep = delay_index.get("departures")
        departures = airport_data.get("airport", {}).get("pluginData", {}).get("schedule", {}).get("departures", {}).get("data", [])
        for entry in departures:
            flight = entry.get("flight")
            if flight:
                row = extract_flight_data(flight, airport_code, airport_name, delay_arr, delay_dep)
                if row:
                    row["flight_day"] = flight_day
                    records.append(row)
    return records

def write_csv(rows):
    output = StringIO()
    fieldnames = [
        "flight_day", "airport_code", "airport_name",
        "delay_index_arrivals", "delay_index_departures",
        "flight_number", "scheduled_departure", "real_departure",
        "delay_minutes"
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()

# === MAIN HANDLER ===

def lambda_handler(event, context):
    # Use today's UTC date by default
    flight_day = datetime.utcnow().date().isoformat()
    key_date = flight_day.replace("-", "")
    json_key = f"{DAILY_JSON_PREFIX}{key_date}.json"

    print(f" Processing {json_key} for {flight_day}")

    # Load JSON for the day
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=json_key)
        raw_json = json.load(obj["Body"])
    except s3.exceptions.NoSuchKey:
        print(f" File not found: {json_key}")
        return {
            "status": "no_file_found",
            "flight_day": flight_day,
            "json_key": json_key
        }

    # Load airport lookup
    airport_csv = s3.get_object(Bucket=BUCKET, Key=AIRPORT_LOOKUP_KEY)['Body'].read().decode("utf-8")
    airport_lookup = load_airport_lookup_from_string(airport_csv)

    # Load existing master
    try:
        master_obj = s3.get_object(Bucket=BUCKET, Key=MASTER_CSV_KEY)
        existing_csv = master_obj["Body"].read().decode("utf-8")
        reader = csv.DictReader(StringIO(existing_csv))
        existing_rows = list(reader)
    except s3.exceptions.NoSuchKey:
        existing_rows = []

    # Deduplication
    all_rows = {
        (r["flight_number"], r["scheduled_departure"], r["flight_day"]): r
        for r in existing_rows
    }

    # Process today's flights
    new_rows = process_json(raw_json, airport_lookup, flight_day)
    for r in new_rows:
        k = (r["flight_number"], r["scheduled_departure"], r["flight_day"])
        all_rows[k] = r

    # Write updated master
    csv_text = write_csv(list(all_rows.values()))
    s3.put_object(Bucket=BUCKET, Key=MASTER_CSV_KEY, Body=csv_text.encode("utf-8"))

    print(f" Processed {len(new_rows)} new flights, total rows: {len(all_rows)}")

    return {
        "status": "success",
        "flight_day": flight_day,
        "rows_added": len(new_rows),
        "total_rows": len(all_rows),
        "json_key": json_key
    }

