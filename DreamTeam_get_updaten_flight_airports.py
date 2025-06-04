import os
import requests
import boto3
from io import BytesIO

# S3-Client initialisieren
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Die Umgebungsvariablen für den S3-Bucket und das Ziel für die CSV-Datei
    s3_bucket_name = os.environ['S3_BUCKET_NAME']
    csv_url = "https://davidmegginson.github.io/ourairports-data/airports.csv"

    try:
        # CSV-Datei herunterladen
        response = requests.get(csv_url)
        
        # Überprüfen, ob der Download erfolgreich war
        if response.status_code == 200:
            # Die heruntergeladene Datei in den S3-Bucket hochladen
            file_name = csv_url.split("/")[-1]  # Der Name der Datei basierend auf der URL
            s3_client.put_object(
                Bucket=s3_bucket_name,
                Key=f"Airport_list/{file_name}",
                Body=BytesIO(response.content),
                ContentType='text/csv'
            )
            return {
                'statusCode': 200,
                'body': f"CSV-Datei erfolgreich in {s3_bucket_name} gespeichert."
            }
        else:
            return {
                'statusCode': 500,
                'body': f"Fehler beim Herunterladen der CSV-Datei: {response.status_code}"
            }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Fehler: {str(e)}"
        }
