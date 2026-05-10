"""
ingestion/producer.py
---------------------
Lambda entry point (also runnable locally).
Polls the TTC GTFS-Realtime vehicle positions feed every 30 seconds,
transforms each vehicle into a clean JSON record, and pushes all records
to Kinesis in a single batched PutRecords call.
"""
import json
import os 
import time 
import boto3 
import requests
from dotenv import load_dotenv 
from google.transit import gtfs_realtime_pb2 

# Load environment variables from .env filed
load_dotenv()

# Helper to fetch a required environment variable or raise an error if missing.
def required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value
REGION      = required_env("AWS_REGION")
STREAM_NAME = required_env("KINESIS_STREAM_NAME")
TTC_URL     = required_env("TTC_VEHICLE_URL")

# Create Kinesis client
kinesis = boto3.client("kinesis", region_name=REGION)

# Function to get TTC vehicle data and return as a list of dicts
def fetch_ttc_positions() -> list[dict]:
    
    # Fetch raw data
    response = requests.get(TTC_URL)
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    records = []

    # Loop though each vehicle in the feed
    for entity in feed.entity:

        # Check for the vehicle entity
        if entity.HasField("vehicle"):

            # Get the vehicle object
            v = entity.vehicle

            # Add vehicle entity to the record
            records.append({
                "vehicle_id": v.vehicle.id,
                "route_id": v.trip.route_id,
                "latitude": v.position.latitude,
                "longitude": v.position.longitude,
                # Convert speed from m/s to km/h
                "speed": v.position.speed * 3.6,
                "timestamp": v.timestamp,
                "ingested_at": int(time.time()),
            })
            

    return records
    

# Function to put all records in Kinesis
def send_to_kinesis(records: list[dict]) -> list[dict]:
    
    # Batch the records into PutRecords calls
    kenisis_records = []

    # Loop through each record and add it to the batch
    for record in records:
        kenisis_records.append({
            'Data': json.dumps(record),  # Convert record to JSON string
            'PartitionKey': record['vehicle_id'] # Parition key to ensure records are sent to the same shard
        })

    print(f"Sending {len(kenisis_records)} vehicle records to {STREAM_NAME}")

    # Put records into Kinesis 500 records at a time
    for i in range(0, len(kenisis_records), 500):

        # Create a batch of records
        chunk = kenisis_records[i:i+500]
        
        # Send records to Kinesis
        response = kinesis.put_records(
            StreamName=STREAM_NAME,
            Records=chunk
        )

        # Check for failed records
        if response['FailedRecordCount'] > 0:
            print(f"Failed to send {response['FailedRecordCount']} records to Kinesis")

        # print how many records were sent
        print(f"Sent {len(chunk)} records to Kinesis")

    return kenisis_records

# Lambda function to put all records in Kinesis for AWS Lambda trigger
def lambda_handler(event: dict, context: object) -> dict:

    # Fetch TTC vehicle positions and send to Kinesis
    print(f"Fetching TTC vehicle positions from {TTC_URL}")
    records = fetch_ttc_positions()
    print(f"Processing {len(records)} vehicle records...")
    
    # Sending record to Kinesis
    send_to_kinesis(records)

    # Return the number of records sent to Kinesis
    return {
        "statusCode": 200,
        "body": json.dumps({"vehicle_ingested": len(records)})
    }


# Main entry
if __name__ == '__main__':

    # Loop every 30 seconds and fetch vehicle positions
    print("Starting TTC vehicle position ingestion...")
    while True:
        lambda_handler({}, None)
        print("Waiting 30 seconds...")
        time.sleep(30) # wait for 30 seconds before fetching the next batch of vehicle positions

    
