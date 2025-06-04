import boto3
import os
import zipfile
import tempfile

s3 = boto3.client('s3')

SOURCE_BUCKET = '1dreamteam1'
SOURCE_PREFIX = 'flightdata/'
ZIP_FILE_NAME = 'flightdata.zip'
DESTINATION_PREFIX = 'zipped/'  # change if you want


def lambda_handler(event, context):
    with tempfile.TemporaryDirectory() as tmpdir:
        # Step 1: Download all files under flightdata/
        response = s3.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=SOURCE_PREFIX)
        if 'Contents' not in response:
            return {"status": "No files found to zip."}

        local_files = []
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('/'):  # Skip folder placeholders
                continue

            local_path = os.path.join(tmpdir, os.path.relpath(key, SOURCE_PREFIX))
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            s3.download_file(SOURCE_BUCKET, key, local_path)
            local_files.append(local_path)

        # Step 2: Zip everything
        zip_path = os.path.join(tmpdir, ZIP_FILE_NAME)
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in local_files:
                arcname = os.path.relpath(file_path, tmpdir)
                zipf.write(file_path, arcname)

        # Step 3: Upload zip to S3
        s3.upload_file(zip_path, SOURCE_BUCKET, f"{DESTINATION_PREFIX}{ZIP_FILE_NAME}")

    return {
        "status": "Success",
        "zip_file": f"s3://{SOURCE_BUCKET}/{DESTINATION_PREFIX}{ZIP_FILE_NAME}"
    }
