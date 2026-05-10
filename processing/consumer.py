"""
processing/consumer.py
----------------------
Lambda function — ETL enrichment + idle detection.
 
Reads vehicle position records from the Kinesis stream, enriches each
record with a processing timestamp and computed metrics then compare the las location stored in DyanmoDB

- if idle for more than 10 minutes send an SNS alert via sns_publisher.py 
- if vehicle move less than 50 meteres, then the vehicle is idle
"""
import base64 # for decoding base64 encoded data
import json 
import os 
import time 
import boto3 # type: ignore
import sys # for debugging locally
import math 
from mypy_boto3_dynamodb import DynamoDBServiceResource

from datetime import datetime, timezone


# Import sns_publisher from alerts module
sys.path.append(os.path.join(os.path.dirname(__file__), "..")) 
from alerts import sns_publisher

# Load environment variables from .env file
from dotenv import load_dotenv



# Get environment variables
load_dotenv()

# Helper to fetch a required environment variable or raise an error if missing.
def required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

REGION = required_env("AWS_REGION")
ENRICHED_STREAM_NAME = required_env("KINESIS_ENRICHED_STREAM_NAME") # ttc-vehicle-enriched
STREAM_NAME = required_env("KINESIS_STREAM_NAME")
TABLE_NAME = required_env("DYNAMODB_TABLE_NAME")
IDLE_THRESHOLD_SECONDS = int(required_env("IDLE_THRESHOLD_MINUTES"))*60 # convert to seconds
IDLE_RADIUS_METRES = int(required_env("IDLE_RADIUS_METRES"))

# AWS Service connections
kinesis = boto3.client("kinesis", region_name=REGION)
dynamodb: DynamoDBServiceResource = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)

#------------------------------------------------------------------------------
# HELPER FUNCTIONS
#------------------------------------------------------------------------------

# Function to calulate haversine distance (distance between two points on a sphere using latitude and longitude)
def haversine_metres(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    
    # Earth radius in metres
    R    = 6_371_000          
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    # Haversine formula
    a    = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2

    # Return the great-circle distance in metres between two GPS points.
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# Function to enrich records with processing timestamp, is_idle, idle_duration
def enrich_record(record: dict) -> dict:
    """
    Add enrichment fields to the record:
    - processing_timestamp
    - is_idle flag (filled in after DynamoDB lookup)
    - idle_duration_seconds (filled in after DynamoDB lookup)
    """

    # Add processing timestamp
    record['processing_timestamp'] = datetime.now(timezone.utc).isoformat()
    record['is_idle'] = False
    record['idle_duration_seconds'] = 0
    
    return record

# Function to return last state of the vehicle, return None if vehicle is not found
def get_last_vehicle_state(vehicle_id: str) -> dict | None:
    response = table.get_item(Key={"vehicle_id": vehicle_id})   
    return response.get("Item")


#------------------------------------------------------------------------------
# IDLE DETECTION FUNCTIONS
#------------------------------------------------------------------------------


# Function to save latest vehicle state
def save_state(vehicle_id: str, lat: float, lon: float, ts: int, idle_since: int | float) -> None:

    # Put item in DynamoDB
    table.put_item(Item={
        'vehicle_id': vehicle_id,
        "latitude":   str(lat),
        "longitude":  str(lon),
        "timestamp": ts,
        "idle_since": int(idle_since),
        "updated_at": int(time.time())
    })


# Function to process Kinesis records
def process_record(record: dict) -> None:

    # Enrich the current record
    record = enrich_record(record)

    # Get current vehicle state
    vehicle_id = record['vehicle_id']
    latitude2  = float(record['latitude'])
    longitude2  = float(record['longitude'])
    ts         = int(record['timestamp'])
    route      = record.get("route_id", "unknown")

    # Get last state of the vehicle
    last = get_last_vehicle_state(vehicle_id)

    # Calculate idle time if last state is found
    if last:

        # Get latitude and longitude of last state
        latitude1 = float(last["latitude"])
        longitude1 = float(last["longitude"])
        
        # Get distance btw point 1,2
        distance = haversine_metres(latitude1, longitude1, latitude2, longitude2)
        idle_since = float(last["idle_since"])

        # Check if vehicle is moving less than 50 metres
        is_idle = distance < IDLE_RADIUS_METRES
        
        
        if is_idle:
            # Calculate idle duration with current time and last state time
            idle_duration = ts - idle_since
            record["is_idle"] = True
            record["idle_duration_seconds"] = idle_duration

            # idle for more than 10 min
            if idle_duration >= IDLE_THRESHOLD_SECONDS:

                print(f"Vehicle {vehicle_id} has been idle for {idle_duration / 60:.2f} minutes on route {route}")

                # Send SNS alert
                sns_publisher.send_alert(
                   vehicle_id,
                   route,
                   latitude2,
                   longitude2,
                   idle_duration
                )
        else: # Moving more than 50 meters

            # Reset position
            idle_since = ts
        
        # Update latest vehicle position
        save_state(vehicle_id, latitude2, longitude2, ts, idle_since)

    else: # First record for this vehicle

        # Save as the first record
        save_state(vehicle_id, latitude2, longitude2, ts, ts)

    # Put enriched record into the enriched Kinesis stream
    kinesis.put_record(
        StreamName=ENRICHED_STREAM_NAME,
        Data=json.dumps(record).encode("utf-8"),
        PartitionKey=vehicle_id
    )

    # Print status message
    print(
        f"Processed vehicle_id: {vehicle_id} | route_id: {route} | "
        f"idle={record['is_idle']} | "
        f"idle_s={record['idle_duration_seconds']}"
    )
        
# Lambda handler to process Kinesis records
def lambda_handler(event:dict, context:object) -> dict:
    records_processed = 0

    # Loop through each record in the event
    for kinesis_record in event.get("Records", []):
        # Decode base64 encoded data and convert to json
        raw    = base64.b64decode(kinesis_record["kinesis"]["data"])
        record = json.loads(raw.decode("utf-8"))
        # Process the record
        process_record(record)
        records_processed += 1 # increment records processed counter
        
    #print(f"Processed {records_processed} records.")
    
    return {"statusCode": 200, "body": json.dumps({"processed": records_processed})}

# Main entry point for local testing
if __name__ == "__main__":
    
    # Read directly from Kinesis
    response   = kinesis.describe_stream(StreamName=STREAM_NAME)

    # Get all shard ids
    shard_ids = [s["ShardId"] for s in response["StreamDescription"]["Shards"]]
    print(f"Reading from stream '{STREAM_NAME}' — {len(shard_ids)} shard(s). Ctrl+C to stop.\n")

    # Loop though each shard and get the latest position
    for shard_id in shard_ids:
        # Get shard iterator
        iterator_response = kinesis.get_shard_iterator(
            StreamName=STREAM_NAME,
            ShardId=shard_id,
            ShardIteratorType="LATEST"
        )

        # Each shard has an iterator, each iterator has a sequence number
        iterator = iterator_response["ShardIterator"]

        # Loop though each record in the stream
        while True:
            
            # Get records
            records_response = kinesis.get_records(ShardIterator=iterator, Limit=100)
            iterator = records_response["NextShardIterator"]

            # Loop through each record and print it
            for rec in records_response["Records"]:
                # Decode base64 encoded data and convert to json
                record = json.loads(rec["Data"].decode("utf-8"))

                # Process the record and save the state of the vehicle in DynamoDB
                process_record(record)

            time.sleep(1) # Wait 1 second between each batch
