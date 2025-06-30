#!/usr/bin/env python3
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import json
import pytz
from datetime import datetime
from io import StringIO
import traceback
import time
import sys
import os

# AWS S3 Setup
s3_client = boto3.client("s3")

# Buckets and prefixes
S3_BUCKET_HELIUS = "pumpfun-websocket-data"
S3_PREFIX_HELIUS = "helius/"   # Updated to match helius API uploads
S3_PREFIX_WEBSOCKET = "Cleaned_websocket_messages/csvs/"
S3_BUCKET_CLEANED = "aws-glue-assets-257394459861-us-west-2"
S3_CSV_ARCHIVE_PREFIX = "Cleaned/csv_archive/"
PARQUET_OUTPUT_KEY = "Helius-Databrew/parquet/combined_csvs.parquet"

def convert_to_pst(utc_timestamp):
    try:
        utc_dt = datetime.utcfromtimestamp(utc_timestamp).replace(tzinfo=pytz.utc)
        pst_dt = utc_dt.astimezone(pytz.timezone("America/Los_Angeles"))
        return pst_dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"Error converting timestamp {utc_timestamp}: {e}")
        return None

def process_helius_transaction(tx):
    records = []
    base_transaction = {
        "Description": "",
        "Type": "Helius",
        "Source": "Helius API",
        "Fee": tx.get("meta", {}).get("fee", 0),
        "Fee Payer": (tx.get("transaction", {}).get("message", {}).get("accountKeys") or [""])[0],
        "Signature": tx.get("signature", ""),
        "Slot": tx.get("slot", 0),
        "Timestamp (PST)": convert_to_pst(tx.get("blockTime", 0)),
        "Token Name": "",
        "Token Symbol": "",
    }
    token_transfers = tx.get("tokenTransfers", [])
    if token_transfers:
        for transfer in token_transfers:
            record = {
                **base_transaction,
                "From Account": transfer.get("fromUserAccount", ""),
                "To Account": transfer.get("toUserAccount", ""),
                "Token Amount": transfer.get("tokenAmount", 0),
                "Mint": transfer.get("mint", ""),
                "Token Standard": transfer.get("tokenStandard", "")
            }
            records.append(record)
    else:
        record = {
            **base_transaction,
            "From Account": "",
            "To Account": "",
            "Token Amount": 0,
            "Mint": "",
            "Token Standard": ""
        }
        records.append(record)
    return records

def process_json_files(bucket, json_files):
    structured_data = []
    for json_file in json_files:
        try:
            response = s3_client.get_object(Bucket=bucket, Key=json_file)
            data = json.load(response["Body"])
        except Exception as e:
            print(f"Error processing {json_file}: {e}")
            continue

        if isinstance(data, dict) and "metadata" in data and "transactions" in data:
            metadata = data.get("metadata", {})
            token_name = metadata.get("token_name", "")
            token_symbol = metadata.get("token_symbol", "")
            mint_address = metadata.get("mint", "")
            transactions = data.get("transactions", [])
            entries = transactions if isinstance(transactions, list) else [transactions]
            for entry in entries:
                base_transaction = {
                    "Description": entry.get("description", ""),
                    "Type": entry.get("type", ""),
                    "Source": entry.get("source", ""),
                    "Fee": entry.get("fee", 0),
                    "Fee Payer": entry.get("feePayer", ""),
                    "Signature": entry.get("signature", ""),
                    "Slot": entry.get("slot", 0),
                    "Timestamp (PST)": convert_to_pst(entry.get("timestamp", 0)),
                    "Token Name": token_name,
                    "Token Symbol": token_symbol,
                }
                if entry.get("tokenTransfers"):
                    for transfer in entry.get("tokenTransfers", []):
                        record = {
                            **base_transaction,
                            "From Account": transfer.get("fromUserAccount", ""),
                            "To Account": transfer.get("toUserAccount", ""),
                            "Token Amount": transfer.get("tokenAmount", 0),
                            "Mint": transfer.get("mint", mint_address),
                            "Token Standard": transfer.get("tokenStandard", "")
                        }
                        structured_data.append(record)
                else:
                    structured_data.append({
                        **base_transaction,
                        "From Account": "",
                        "To Account": "",
                        "Token Amount": 0,
                        "Mint": mint_address,
                        "Token Standard": ""
                    })
        elif isinstance(data, list):
            # Process list of transactions from Helius API
            for tx in data:
                records = process_helius_transaction(tx)
                structured_data.extend(records)
        else:
            print(f"Unrecognized JSON structure in {json_file}")

        try:
            s3_client.delete_object(Bucket=bucket, Key=json_file)
            print(f"Deleted JSON file: {json_file}")
        except Exception as e:
            print(f"Failed to delete {json_file}: {e}")
    return pd.DataFrame(structured_data)

def rename_csv_files_to_timestamp_format():
    continuation_token = None
    while True:
        params = {"Bucket": S3_BUCKET_CLEANED, "Prefix": S3_CSV_ARCHIVE_PREFIX}
        if continuation_token:
            params["ContinuationToken"] = continuation_token
        response = s3_client.list_objects_v2(**params)
        contents = response.get("Contents", [])
        if not contents:
            print("No CSV objects found for renaming.")
            break
        for obj in contents:
            old_key = obj["Key"]
            if old_key.endswith(".csv"):
                filename = old_key.split("/")[-1]
                if len(filename) >= 14 and filename[:14].isdigit():
                    continue
                last_modified = obj["LastModified"].astimezone(pytz.timezone("America/Los_Angeles"))
                date_str = last_modified.strftime("%Y%m%d%H%M%S")
                new_filename = f"{date_str}_cleaned_transactions.csv"
                new_key = f"{S3_CSV_ARCHIVE_PREFIX}{new_filename}"
                if new_key != old_key:
                    print(f"Renaming {old_key} -> {new_key}")
                    try:
                        s3_client.copy_object(
                            Bucket=S3_BUCKET_CLEANED,
                            CopySource={'Bucket': S3_BUCKET_CLEANED, 'Key': old_key},
                            Key=new_key
                        )
                        s3_client.delete_object(Bucket=S3_BUCKET_CLEANED, Key=old_key)
                    except Exception as e:
                        print(f"Error renaming file {old_key}: {e}")
        if response.get("IsTruncated"):
            continuation_token = response["NextContinuationToken"]
        else:
            break

def list_all_csv_websocket_files(bucket, prefix):
    all_csv_files = []
    continuation_token = None
    while True:
        params = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            params["ContinuationToken"] = continuation_token
        response = s3_client.list_objects_v2(**params)
        for obj in response.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv"):
                all_csv_files.append(key)
        if response.get("IsTruncated"):
            continuation_token = response["NextContinuationToken"]
        else:
            break
    return all_csv_files

def process_websocket_csv_files(bucket, csv_files):
    structured_data = []
    for csv_file in csv_files:
        try:
            response = s3_client.get_object(Bucket=bucket, Key=csv_file)
            csv_content = response["Body"].read().decode("utf-8")
            df = pd.read_csv(StringIO(csv_content))
        except Exception as e:
            print(f"Error processing {csv_file}: {e}")
            continue

        if "mint" not in df.columns:
            print(f"Skipping {csv_file}: 'mint' column not found.")
            continue

        for _, row in df.iterrows():
            record = {
                "Description": "",
                "Type": "",
                "Source": "",
                "Fee": 0,
                "Fee Payer": "",
                "Signature": "",
                "Slot": 0,
                "Timestamp (PST)": "",
                "Token Name": row.get("name", ""),
                "Token Symbol": row.get("symbol", ""),
                "From Account": "",
                "To Account": "",
                "Token Amount": 0,
                "Mint": row.get("mint", ""),
                "Token Standard": ""
            }
            structured_data.append(record)
        try:
            s3_client.delete_object(Bucket=bucket, Key=csv_file)
            print(f"Deleted websocket CSV file: {csv_file}")
        except Exception as e:
            print(f"Failed to delete websocket CSV file {csv_file}: {e}")
    return pd.DataFrame(structured_data)

def list_all_json_files(bucket, prefix):
    all_json_files = []
    continuation_token = None
    while True:
        params = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            params["ContinuationToken"] = continuation_token
        response = s3_client.list_objects_v2(**params)
        for obj in response.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                all_json_files.append(key)
        if response.get("IsTruncated"):
            continuation_token = response["NextContinuationToken"]
        else:
            break
    return all_json_files

def main():
    # Process JSON files from Helius API data
    json_files = list_all_json_files(S3_BUCKET_HELIUS, S3_PREFIX_HELIUS)
    if json_files:
        print(f"Found {len(json_files)} JSON files.")
        df_cleaned = process_json_files(S3_BUCKET_HELIUS, json_files)
    else:
        print("No JSON files found.")
        df_cleaned = pd.DataFrame()

    # Process websocket CSV files
    websocket_csv_files = list_all_csv_websocket_files(S3_BUCKET_HELIUS, S3_PREFIX_WEBSOCKET)
    if websocket_csv_files:
        print(f"Found {len(websocket_csv_files)} websocket CSV files.")
        df_websocket = process_websocket_csv_files(S3_BUCKET_HELIUS, websocket_csv_files)
        if not df_cleaned.empty:
            df_cleaned = pd.concat([df_cleaned, df_websocket], ignore_index=True)
        else:
            df_cleaned = df_websocket
    else:
        print("No websocket CSV files found.")

    # Upload cleaned CSV to archive
    csv_buffer = StringIO()
    try:
        df_cleaned.to_csv(csv_buffer, index=False)
    except Exception as e:
        print(f"Error converting cleaned DataFrame to CSV: {e}")
        return
    csv_data = csv_buffer.getvalue()
    csv_key = f"{S3_CSV_ARCHIVE_PREFIX}cleaned_transactions_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    try:
        s3_client.put_object(Bucket=S3_BUCKET_CLEANED, Key=csv_key, Body=csv_data.encode('utf-8'))
        print(f"Uploaded cleaned CSV to s3://{S3_BUCKET_CLEANED}/{csv_key}")
    except Exception as e:
        print(f"Error uploading cleaned CSV: {e}")

    rename_csv_files_to_timestamp_format()

    # Convert CSVs to Parquet
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET_CLEANED, Prefix=S3_CSV_ARCHIVE_PREFIX)
    csv_objects = response.get("Contents", [])
    csv_files = [obj for obj in csv_objects if obj["Key"].endswith(".csv")]

    if not csv_files:
        print("No CSV files found for Parquet conversion.")
        return

    csv_files_sorted = sorted(csv_files, key=lambda x: x["LastModified"], reverse=True)
    recent_csv_files = csv_files_sorted[:200]
    recent_csv_files = sorted(recent_csv_files, key=lambda x: x["LastModified"])

    parquet_buffer = io.BytesIO()
    writer = None
    schema = None

    for csv_obj in recent_csv_files:
        csv_key = csv_obj["Key"]
        print(f"Processing CSV: s3://{S3_BUCKET_CLEANED}/{csv_key}")
        try:
            response = s3_client.get_object(Bucket=S3_BUCKET_CLEANED, Key=csv_key)
            csv_content = response["Body"].read().decode("utf-8")
            if not csv_content.strip():
                raise ValueError("Empty file")
            df = pd.read_csv(StringIO(csv_content))
            for col in ["Fee", "Slot", "Token Amount"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        except Exception as e:
            if "No columns to parse from file" in str(e) or "Empty file" in str(e):
                print(f"Error reading {csv_key}: {e}. Deleting file.")
                try:
                    s3_client.delete_object(Bucket=S3_BUCKET_CLEANED, Key=csv_key)
                    print(f"Deleted problematic file: {csv_key}")
                except Exception as delete_error:
                    print(f"Failed to delete problematic file {csv_key}: {delete_error}")
            else:
                print(f"Error reading {csv_key}: {e}")
            continue

        try:
            table = pa.Table.from_pandas(df)
        except Exception as e:
            print(f"Error converting DataFrame to Arrow Table for {csv_key}: {e}")
            continue

        try:
            if writer is None:
                schema = table.schema
                writer = pq.ParquetWriter(parquet_buffer, schema)
            else:
                if table.schema != schema:
                    missing_cols = set(schema.names) - set(table.schema.names)
                    for col in missing_cols:
                        table = table.append_column(col, pa.nulls(len(table)))
                    table = table.select(schema.names)
                    table = pa.Table.from_arrays([table[col] for col in schema.names], schema=schema)
            writer.write_table(table)
            print(f"Processed CSV: {csv_key}")
        except Exception as e:
            print(f"Error writing table for {csv_key}: {e}")
            continue

    if writer:
        try:
            writer.close()
        except Exception as e:
            print(f"Error closing Parquet writer: {e}")

    parquet_buffer.seek(0)
    try:
        s3_client.upload_fileobj(parquet_buffer, S3_BUCKET_CLEANED, PARQUET_OUTPUT_KEY)
        print(f"Uploaded combined Parquet file to s3://{S3_BUCKET_CLEANED}/{PARQUET_OUTPUT_KEY}")
    except Exception as e:
        print(f"Error uploading Parquet file: {e}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("An error occurred in the main process:")
        traceback.print_exc()
