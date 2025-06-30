import time
import subprocess
import boto3
import json
import requests
import csv
import io
from datetime import datetime
import traceback

# AWS S3 Setup
S3_BUCKET = "pumpfun-websocket-data"
S3_SOURCE_PREFIX = "Cleaned_websocket_messages/csvs/"  # Source for mint extraction
S3_DEST_PREFIX = "helius/"  # Destination for Helius API data
s3_client = boto3.client("s3")

# Helius API Setup
import os
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
HELIUS_API_URL = "https://api.helius.xyz/v0/addresses/{address}/transactions/?api-key=" + HELIUS_API_KEY

def get_mint_addresses_from_s3():
    print(f"Checking for mint addresses in s3://{S3_BUCKET}/{S3_SOURCE_PREFIX}...")
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_SOURCE_PREFIX)
    if "Contents" not in response:
        print("No CSV files found in S3 bucket!")
        return []

    mint_addresses = set()
    csv_files = [obj for obj in response["Contents"] if obj["Key"].endswith(".csv") and "cleaned_transactions" not in obj["Key"]]
    sorted_files = sorted(csv_files, key=lambda x: x["LastModified"], reverse=True)
    latest_files = sorted_files[:1000]
    for obj in latest_files:
        file_key = obj["Key"]
        print(f"Reading {file_key}...")
        obj_data = s3_client.get_object(Bucket=S3_BUCKET, Key=file_key)
        csv_content = obj_data["Body"].read().decode("utf-8")
        f = io.StringIO(csv_content)
        reader = csv.DictReader(f)
        if reader.fieldnames and "mint" in reader.fieldnames:
            for row in reader:
                if row.get("mint"):
                    mint_addresses.add(row["mint"])
        else:
            print(f"Skipping {file_key} as it does not contain a 'mint' column.")
        s3_client.delete_object(Bucket=S3_BUCKET, Key=file_key)
        print(f"Deleted {file_key}")
    print(f"Found {len(mint_addresses)} unique mint addresses.")
    return list(mint_addresses)

def check_existing_mint(mint):
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{S3_DEST_PREFIX}{mint}.json")
    return "Contents" in response

def fetch_helius_data(mint_address):
    print(f"Fetching transaction history for {mint_address}...")
    url = HELIUS_API_URL.format(address=mint_address) + "&limit=100"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching data for {mint_address}: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {mint_address}: {e}")
        return None

def upload_to_s3(data, mint_address):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{S3_DEST_PREFIX}helius_transactions_{mint_address}_{timestamp}.json"
    if data:
        s3_client.put_object(
            Bucket=S3_BUCKET, 
            Key=filename, 
            Body=json.dumps(data), 
            ContentType="application/json"
        )
        print(f"Uploaded to S3: s3://{S3_BUCKET}/{filename}")
    else:
        print(f"No data for {mint_address}, skipping upload.")

def main():
    mint_addresses = get_mint_addresses_from_s3()
    if not mint_addresses:
        print("No mint addresses found. Exiting...")
        return

    for mint in mint_addresses:
        if check_existing_mint(mint):
            print(f"Skipping {mint}, already processed.")
            continue
        transactions = fetch_helius_data(mint)
        upload_to_s3(transactions, mint)
        time.sleep(1)

if __name__ == "__main__":
    while True:
        try:
            main()
            print("Data fetch complete! Running clean_data.py...")
            subprocess.run(["python3", "clean_data.py"])
        except Exception as e:
            print("Exception occurred:")
            traceback.print_exc()
        finally:
            print("Restarting in 5 seconds...")
            time.sleep(5)
