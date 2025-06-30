import threading
import time
import subprocess
import websocket
import json
import boto3
import requests
from datetime import datetime

# AWS S3 Setup
S3_SOURCE_BUCKET = "pumpfun-websocket-data"
S3_SOURCE_PREFIX = "websocket_messages/"
S3_DEST_BUCKET = "pumpfun-websocket-data"
S3_DEST_FOLDER = "Helius/"
S3_PROCESSED_TRANSACTIONS = "Helius/processed_transactions.json"
s3 = boto3.client("s3")

# WebSocket URL
WS_URL = "wss://pumpportal.fun/api/data"

# Messages to send on connect
SUBSCRIBE_MESSAGES = [
    json.dumps({"method": "subscribeRaydiumLiquidity"}),
]

# Helius API Setup
HELIUS_API_KEY = "bae5f016-3bfc-43d2-831c-4b4eb48d8776"
HELIUS_API_URL = "https://api.helius.xyz/v0/addresses/{address}/transactions/?api-key=" + HELIUS_API_KEY

def save_to_s3(data):
    """Save incoming WebSocket message to S3"""
    timestamp = int(time.time() * 1000)
    file_key = f"websocket_messages/{timestamp}.json"

    try:
        s3.put_object(
            Bucket=S3_SOURCE_BUCKET,
            Key=file_key,
            Body=json.dumps(data, indent=2),
            ContentType="application/json"
        )
        print(f"Saved message to S3: {file_key}")
    except Exception as e:
        print(f"Error saving to S3: {e}")

def on_message(ws, message):
    """Handle incoming WebSocket message"""
    try:
        data = json.loads(message)
        print("Received message:", data)
        save_to_s3(data)
    except json.JSONDecodeError:
        print("Error decoding message")

def on_open(ws):
    """Send subscription messages on WebSocket open"""
    print("📡 WebSocket connection opened, subscribing...")
    for message in SUBSCRIBE_MESSAGES:
        ws.send(message)

def start_websocket():
    """Start WebSocket connection"""
    ws = websocket.WebSocketApp(
        WS_URL,
        on_message=on_message,
        on_open=on_open
    )
    ws.run_forever()

def get_processed_transactions():
    """Retrieve processed transaction IDs from S3 to avoid re-downloading old data."""
    try:
        obj = s3.get_object(Bucket=S3_DEST_BUCKET, Key=S3_PROCESSED_TRANSACTIONS)
        return json.load(obj["Body"])
    except s3.exceptions.NoSuchKey:
        print("ℹ️ No processed transactions found. Starting fresh...")
        return {}

def save_processed_transactions(processed_txns):
    """Save the list of processed transactions to S3."""
    s3.put_object(
        Bucket=S3_DEST_BUCKET,
        Key=S3_PROCESSED_TRANSACTIONS,
        Body=json.dumps(processed_txns, indent=2),
        ContentType="application/json"
    )

def fetch_helius_data(mint, processed_txns):
    """Fetch only new transactions for the given mint address."""
    url = HELIUS_API_URL.format(address=mint)
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"❌ Failed to fetch data for {mint}. Status: {response.status_code}")
        return []

    transactions = response.json()
    new_transactions = []

    for txn in transactions:
        txn_id = txn.get("signature")
        if txn_id and txn_id not in processed_txns.get(mint, set()):
            new_transactions.append(txn)
            processed_txns.setdefault(mint, set()).add(txn_id)

    return new_transactions

def upload_to_s3(data, mint):
    """Upload new transactions to S3."""
    if not data:
        print(f"⚠️ No new transactions for {mint}, skipping upload.")
        return

    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    file_key = f"{S3_DEST_FOLDER}{mint}_{timestamp}.json"
    
    s3.put_object(
        Bucket=S3_DEST_BUCKET,
        Key=file_key,
        Body=json.dumps(data, indent=2),
        ContentType="application/json"
    )
    
    print(f"📤 Uploaded {len(data)} new transactions for {mint} to S3: {file_key}")

def run_helius2():
    """Fetch and upload new Solana transactions for mint addresses."""
    processed_txns = get_processed_transactions()
    mint_addresses = ["address1", "address2"]  # Replace with actual address fetching logic

    for mint in mint_addresses:
        print(f"📡 Fetching new transactions for {mint}...")
        new_transactions = fetch_helius_data(mint, processed_txns)
        upload_to_s3(new_transactions, mint)
        time.sleep(1)
    
    save_processed_transactions(processed_txns)
    print("✅ Data fetch complete! Running clean_data.py...")
    subprocess.run(["python3", "clean_data.py"])  # Run clean_data.py after finishing data processing

def run_helius2_every_three_hours():
    while True:
        print("⏳ Running helius2.py...")
        run_helius2()
        print("✅ Completed helius2.py. Waiting 3 hours...")
        time.sleep(10800)

if __name__ == "__main__":
    thread = threading.Thread(target=start_websocket)
    thread.start()
    run_helius2_every_three_hours()
