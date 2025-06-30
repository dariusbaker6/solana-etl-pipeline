import boto3
import json
import pandas as pd
import time
from io import StringIO

# Constants
BUCKET_NAME = 'pumpfun-websocket-data'
SOURCE_PREFIX = 'websocket_messages/'
DEST_PREFIX = 'Cleaned_websocket_messages/csvs/'
BATCH_SIZE = 999
WAIT_SECONDS = 5
JSON_THRESHOLD = 500  # Threshold to trigger deletion of all JSON files

# S3 Client
s3 = boto3.client('s3')

def list_json_files():
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=SOURCE_PREFIX)
    files = response.get('Contents', [])
    json_files = [f for f in files if f['Key'].endswith('.json') and DEST_PREFIX not in f['Key']]
    json_files.sort(key=lambda x: x['Key'], reverse=True)
    return json_files

def transform_json_to_csv(json_content):
    data = json.loads(json_content)
    filtered_data = {
        'mint': data.get('mint'),
        'txType': data.get('txType'),
        'solAmount': data.get('solAmount'),
        'name': data.get('name'),
        'symbol': data.get('symbol'),
    }
    return pd.DataFrame([filtered_data])

def process_batch(batch_files):
    for file_obj in batch_files:
        key = file_obj['Key']
        try:
            response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            content = response['Body'].read().decode('utf-8')
            df = transform_json_to_csv(content)
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            base_filename = key.split('/')[-1].replace('.json', '.csv')
            dest_key = f"{DEST_PREFIX}{base_filename}"
            s3.put_object(Bucket=BUCKET_NAME, Key=dest_key, Body=csv_buffer.getvalue())
            s3.delete_object(Bucket=BUCKET_NAME, Key=key)
            print(f"Processed: {key} → {dest_key}")
        except Exception as e:
            print(f"Error processing {key}: {e}")

def delete_all_json_files(json_files):
    for file_obj in json_files:
        key = file_obj['Key']
        try:
            s3.delete_object(Bucket=BUCKET_NAME, Key=key)
            print(f"Deleted JSON file: {key}")
        except Exception as e:
            print(f"Error deleting {key}: {e}")

def run_loop():
    while True:
        try:
            files = list_json_files()
            if files:
                print(f"Found {len(files)} files. Processing up to {BATCH_SIZE} files.")
                batch = files[:BATCH_SIZE]
                process_batch(batch)
                if len(files) >= JSON_THRESHOLD:
                    print(f"Threshold reached ({len(files)} files). Deleting all JSON files.")
                    delete_all_json_files(files)
            else:
                print("No new files found.")
        except Exception as e:
            print(f"Unexpected error in loop: {e}")
        time.sleep(WAIT_SECONDS)

if __name__ == "__main__":
    run_loop()
