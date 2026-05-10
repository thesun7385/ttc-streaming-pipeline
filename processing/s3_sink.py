# Todo: Complate sink function
"""
processing/s3_sink.py
---------------------
Lambda function — S3 data lake sink.
 
Reads vehicle position records from the Kinesis stream, converts
each batch to Parquet using PyArrow, and writes to S3 partitioned
by year / month / day / route. 
 
Output path pattern:
  s3://{S3_BUCKET_NAME}/
    year=YYYY/
      month=MM/
        day=DD/
          route={route_id}/
            vehicles_{unix_ts}.parquet
 
Environment variables:
  AWS_DEFAULT_REGION  — must be us-east-1
  S3_BUCKET_NAME      — ttc-data-lake-{account_id}
"""

import base64
import io # Used to work with binary data in memory
import json
import os
import time
from datetime import datetime, timezone
import boto3 
# PyArrow used for working with Parquet files
import pyarrow as pa
import pyarrow.parquet as pq

from dotenv import load_dotenv

# Load env
load_dotenv()
REGION = os.getenv("AWS_REGION")
ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# S3 Connection
s3 = boto3.client("s3", region_name=REGION)

# Table Schema
SCHEMA = pa.schema([
    pa.field("vehicle_id", pa.string()),
    pa.field("route_id", pa.string()),
    pa.field("latitude", pa.float64()),
    pa.field("longitude", pa.float64()),
    pa.field("speed_kmh", pa.float64()),
    pa.field("timestamp", pa.int64()),
    pa.field("ingested_at", pa.int64()),
    pa.field("processing_timestamp", pa.string()),
    pa.field("is_idle", pa.bool_()),
    pa.field("idle_duration_seconds", pa.int64()),
])

# Function to transform records to parquet
def records_to_parquet(records: list[dict]) -> bytes:

    # Cast type for each field and handle missing values
    typed = {
        "vehicle_id": [str(r.get("vehicle_id")) for r in records],
        "route_id": [str(r.get("route_id", "unknown")) for r in records], # If route not found, default to unknown
        "latitude": [float(r.get("latitude", 0)) for r in records], 
        "longitude": [float(r.get("longitude", 0)) for r in records], 
        "speed_kmh": [round(float(r.get("speed",0)),3) for r in records], 
        "timestamp": [int(r.get("timestamp", 0)) for r in records], 
        "ingested_at": [int(r.get("ingested_at", 0)) for r in records], 
        "processing_timestamp": [str(r.get("processing_timestamp", "unknown")) for r in records],
        "is_idle": [bool(r.get("is_idle",False)) for r in records],
        "idle_duration_seconds": [int(r.get("idle_duration_seconds",0)) for r in records],
    }

    # Convert to Arrow Table with proper schema
    table = pa.table(typed, schema=SCHEMA)

    # Serialize to Parquet in-memory buffer
    buffer = io.BytesIO()
    # 
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)
    # Return the binary content of the buffer
    return buffer.getvalue()


# Function to receive route_id and timestamp and return s3 key
def build_s3_key(route_id: str, ts: int) -> str:

    # Convert batch timestamp to datetime
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return (
        f"year={dt.strftime('%Y')}/"
        f"month={dt.strftime('%m')}/"
        f"day={dt.strftime('%d')}/"
        f"route={route_id}/"
        f"vehicles_{ts}.parquet"
    )

# Function to sink records to s3 (write method)
def sink_batch_to_s3(records: list[dict]) -> None:

    # Create empty to group records by route_id
    by_route: dict[str, list[dict]] = {}  
    # Loop through all records
    for r in records:
        # Get route_id and assign unknown if no route
        route = r.get("route_id", "unknown")

        # Add record to route, set empty as default
        by_route.setdefault(route, []).append(r)

    now = int(time.time())

    # Loop though route_id and records 
    for route_id, route_records in by_route.items():

        # Convert batch to parquet
        parquet_bytes = records_to_parquet(route_records)
        s3_key = build_s3_key(route_id, now)

        # Upload to S3
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key, # partition key 
            Body=parquet_bytes, # parquet data
            ContentType="application/octet-stream",
        )

        print(
            f"  ✓ Wrote {len(route_records)} records → "
            f"s3://{S3_BUCKET_NAME}/{s3_key} "
            f"({len(parquet_bytes):,} bytes)"
        )
        

# Lamba function to extract and process data from kinesis
def lambda_handler(event: dict, context) -> dict:

    records = []

    # Get record from kinesis event 
    for kinesis_record in event.get("Records", []):
        #Decode base64 event data
        raw = base64.b64decode(kinesis_record['kinesis']['data'])
        record = json.loads(raw.decode('utf-8'))
        records.append(record)

    # Filter out any empty records
    print(f"Sinking {len(records)} records to s3://{S3_BUCKET_NAME}")
    sink_batch_to_s3(records)
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Successfully sunk records to S3",
            "records_processed": len(records),
        })
    }

# Local entry point
if __name__ == "__main__":

    # Local test: reads from Kinesis and sinks to S3
    ENRICHED_STREAM_NAME = os.getenv("KINESIS_ENRICHED_STREAM_NAME") # ttc-vehicle-enriched
    kinesis = boto3.client("kinesis", region_name=REGION)

    # Get resoinse and shard_id
    response  = kinesis.describe_stream(StreamName=ENRICHED_STREAM_NAME)
    shard_ids = [s["ShardId"] for s in response["StreamDescription"]["Shards"]]
    print(f"Reading from '{ENRICHED_STREAM_NAME}', sinking to s3://{S3_BUCKET_NAME}/. Ctrl+C to stop.\n")

    # Loop to kinesis shards 
    for shard_id in shard_ids:

        # Get shard iterator
        it_resp  = kinesis.get_shard_iterator(
            StreamName=ENRICHED_STREAM_NAME, ShardId=shard_id, ShardIteratorType="LATEST"
        )
        iterator = it_resp["ShardIterator"]

        # Loop though shards and save data into S3
        while True:
            resp = kinesis.get_records(ShardIterator=iterator, Limit=100)
            iterator = resp["NextShardIterator"]
            records  = [json.loads(r["Data"].decode("utf-8")) for r in resp["Records"]]

            # If found records -> upload to s3
            if records:
                sink_batch_to_s3(records)
            else:
                # Prevent infinite loops
                print("No records found in this shard, waiting 5 seconds...")
        
            time.sleep(5)
    
  
    

