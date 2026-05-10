
"""
alerts/sns_publisher.py
-----------------------
Shared SNS alert helper.
Called by processing/consumer.py whenever a vehicle is detected as idle
for longer than IDLE_THRESHOLD_MINUTES (10 mins).
 
Keeping SNS logic here means the consumer stays focused on detection,
and alert formatting can be updated in one place.

"""

import os
import boto3
from dotenv import load_dotenv

# Load environment variables


load_dotenv() 

REGION = os.getenv("AWS_REGION")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")

# Create SNS client
sns = boto3.client("sns", region_name=REGION)


# Function to publish alert to SNS
def send_alert(
    vehicle_id: str,
    route_id: str,
    latitude: float,
    longitude: float,
    idle_seconds: float
) -> None:

    # Calculate idle minutes
    idle_minutes = round(idle_seconds / 60, 1)

    # Build SNS message payload
    subject = f"TTC Idle Alert: {vehicle_id} - Route {route_id}"
    maps_link = f"https://www.google.com/maps?q={latitude},{longitude}"
    message = f"""
    Vehicle {vehicle_id} has been idle for {idle_minutes} minutes.
    Route: {route_id}
    Location: {latitude}, {longitude}
    View on Map: {maps_link}
    """

    # Send an alert via SNS
    response = sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message
    )

    # Print success message
    print("Idle alert sent via SNS: ", response["MessageId"])

