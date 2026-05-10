# TTC Real-Time Streaming Pipeline

A cost-optimized, event-driven data pipeline that ingests live GPS positions from every active TTC bus and streetcar, detects vehicles idle for more than 10 minutes, fires SNS alerts, and sinks the full stream to a queryable S3 data lake.

---

## Architecture

```
TTC GTFS-RT Feed (free)
        │
        │  poll every 30s (peak hours only)
        ▼
EventBridge Scheduler ──► Lambda Poller          ingestion/producer.py
                                │
                                │  PutRecords (batched)
                                ▼
                     Kinesis Data Stream          1 shard · provisioned · us-east-1
                                │
                    ┌───────────┴────────────┐
                    ▼                        ▼
          Lambda ETL + Detector      Lambda S3 Sink
          processing/consumer.py     processing/s3_sink.py
                    │                        │
          ┌─────────┴──────┐                 ▼
          ▼                ▼           S3 Data Lake (Parquet)
       DynamoDB          SNS Alert     analysis/athena_queries.sql
    (vehicle state)   (idle > 10 min)
          ▲
          │
    alerts/sns_publisher.py
```

**Data source:** [`bustime.ttc.ca/gtfsrt/vehicles`](https://bustime.ttc.ca/gtfsrt/vehicles) — free, no API key, licensed under the Open Government Licence – Toronto.

---

## Cost Breakdown

| Service                  | Cost              |
| ------------------------ | ----------------- |
| TTC GTFS-RT data         | $0.00             |
| Lambda (all functions)   | $0.00 (free tier) |
| DynamoDB (vehicle state) | $0.00 (free tier) |
| SNS alerts               | $0.00 (free tier) |
| S3 + Athena              | ~$0.05 / mo       |
| Kinesis (3 hrs/wk demo)  | ~$0.43 / mo       |
| **Total**                | **~$0.48 / mo**   |

> Kinesis has no free tier. The key cost strategy is to **delete the stack after each demo session** using `stop_demo.sh` and recreate it before the next using `start_demo.sh`. See [Cost Strategy](#cost-strategy) below.

---

## Project Structure

```
ttc-streaming-pipeline/
│
├── infrastructure/
│   └── template.yaml          # CloudFormation — all AWS resources
│                              # replaces setup.py + teardown.py
│
├── ingestion/
│   └── producer.py            # Lambda: polls TTC feed → Kinesis
│
├── processing/
│   ├── consumer.py            # Lambda: idle detection → SNS + DynamoDB
│   └── s3_sink.py             # Lambda: buffers records → S3 Parquet
│
├── alerts/
│   └── sns_publisher.py       # Shared SNS alert helper
│
├── analysis/
│   └── athena_queries.sql     # Sample queries for pattern analysis
│
└── scripts/
    ├── start_demo.sh          # Calls: aws cloudformation deploy
    └── stop_demo.sh           # Calls: aws cloudformation delete-stack
```

---

## Prerequisites

- Python 3.11+
- AWS CLI v2 configured with an IAM user that has permissions for Kinesis, DynamoDB, S3, SNS, Lambda, CloudFormation, and EventBridge

```bash
# Verify your AWS CLI is configured correctly
aws sts get-caller-identity

# Expected output
{
    "UserId": "AIDAXXXXXXXXXXXXXXXXX",
    "Account": "123456789012",
    "Arn": "arn:aws:iam::123456789012:user/your-user"
}
```

---

## Quick Start

### 1. Clone and install dependencies

```bash
git clone https://github.com/your-username/ttc-streaming-pipeline.git
cd ttc-streaming-pipeline
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env` with your values:

```bash
AWS_DEFAULT_REGION=us-east-1
AWS_ACCOUNT_ID=123456789012
KINESIS_STREAM_NAME=ttc-vehicle-positions
DYNAMODB_TABLE_NAME=ttc-vehicle-state
SNS_TOPIC_ARN=arn:aws:sns:us-east-1:123456789012:ttc-idle-alerts
S3_BUCKET_NAME=ttc-data-lake-123456789012
IDLE_THRESHOLD_MINUTES=10
```

### 3. Deploy infrastructure

```bash
bash scripts/start_demo.sh
```

This runs:

```bash
aws cloudformation deploy \
  --template-file infrastructure/template.yaml \
  --stack-name ttc-streaming-pipeline \
  --region us-east-1 \
  --parameter-overrides AccountId=YOUR_ACCOUNT_ID
```

> `infrastructure/template.yaml` enforces `us-east-1` via a CloudFormation `Rules` block. Deploying to any other region fails immediately before any resource is created.

### 4. Start streaming

```bash
python ingestion/producer.py
```

You should see:

```
Fetched 687 vehicles from TTC
Sent 500 records. Failed: 0
Sent 187 records. Failed: 0
```

### 5. Tear down after your session

```bash
bash scripts/stop_demo.sh
```

Deletes the entire CloudFormation stack. All resources are removed in the correct dependency order and **billing stops immediately**.

---

## Components

### `infrastructure/template.yaml`

CloudFormation template that provisions all AWS resources in `us-east-1`. Replaces both `setup.py` and `teardown.py` — the stack is the single source of truth for your infrastructure.

| Logical ID          | AWS Resource        | Configuration               |
| ------------------- | ------------------- | --------------------------- |
| `TTCVehicleStream`  | Kinesis Data Stream | 1 shard, provisioned        |
| `VehicleStateTable` | DynamoDB Table      | PAY_PER_REQUEST (free tier) |
| `DataLakeBucket`    | S3 Bucket           | Standard storage            |
| `IdleAlertTopic`    | SNS Topic           | Email subscription          |

A `Rules` block prevents deployment outside `us-east-1`:

```yaml
Rules:
  EnforceUsEast1:
    Assertions:
      - Assert: !Equals [!Ref "AWS::Region", "us-east-1"]
        AssertDescription: "This stack must be deployed in us-east-1."
```

---

### `ingestion/producer.py`

Lambda function triggered by EventBridge every 30 seconds during peak hours. Fetches the live TTC GTFS-Realtime vehicle position feed, transforms each vehicle entry into a clean JSON record, and pushes all records to Kinesis in a single batched `PutRecords` call.

Batching all ~700 vehicles into one call avoids Kinesis's 1KB minimum billing rounding being applied per record.

**Output record schema:**

```json
{
  "vehicle_id": "1234",
  "route_id": "504",
  "latitude": 43.6532,
  "longitude": -79.3832,
  "speed_kmh": 34.2,
  "timestamp": 1746123456,
  "ingested_at": 1746123461
}
```

---

### `processing/consumer.py`

Lambda function that reads from Kinesis and performs two jobs:

**1. ETL enrichment** — adds `processing_timestamp` and computed metrics to each record.

**2. Idle detection** — compares the vehicle's current position against its last known position in DynamoDB. If the vehicle has not moved more than 50 metres within the idle threshold, it calls `alerts/sns_publisher.py`.

```
distance = haversine(last_lat, last_lon, new_lat, new_lon)
if distance < 50m AND (now - idle_since) > IDLE_THRESHOLD_MINUTES * 60:
    sns_publisher.send_alert(vehicle_id, route, lat, lon, duration)
```

---

### `processing/s3_sink.py`

Lambda function that reads from Kinesis, buffers records, converts them to Parquet using PyArrow, and writes to S3 partitioned by date and route:

```
s3://ttc-data-lake-{account}/
  year=2025/
    month=05/
      day=02/
        route=504/
          vehicles_1746123456.parquet
```

This partition layout lets Athena skip irrelevant partitions, reducing query cost to pennies.

---

### `alerts/sns_publisher.py`

Shared helper module used by `processing/consumer.py` to publish idle vehicle alerts to the SNS topic. Keeping SNS logic in one place means it can be updated without touching the consumer.

**Alert message format:**

```
IDLE ALERT: Vehicle 1234 on route 504
has been stationary for 12.5 minutes.
Location: 43.65320, -79.38320
Maps: https://maps.google.com/?q=43.65320,-79.38320
```

---

### `analysis/athena_queries.sql`

Sample SQL queries to run against the S3 data lake via Athena for long-term pattern analysis.

```sql
-- Top 10 most frequently idle routes
SELECT route_id,
       COUNT(*)                         AS idle_events,
       AVG(idle_duration_seconds) / 60  AS avg_idle_minutes
FROM   ttc_vehicle_positions
WHERE  is_idle = true
GROUP  BY route_id
ORDER  BY idle_events DESC
LIMIT  10;

-- Idle events by hour of day
SELECT EXTRACT(HOUR FROM from_unixtime(timestamp)) AS hour_of_day,
       COUNT(*) AS idle_count
FROM   ttc_vehicle_positions
WHERE  is_idle = true
GROUP  BY 1
ORDER  BY 1;
```

---

### `scripts/start_demo.sh`

Deploys the CloudFormation stack to `us-east-1`. Safe to run repeatedly — CloudFormation skips resources that already exist.

```bash
bash scripts/start_demo.sh
```

---

### `scripts/stop_demo.sh`

Deletes the CloudFormation stack and all resources inside it. Run this after every demo session to stop Kinesis billing.

```bash
bash scripts/stop_demo.sh
```

---

## Cost Strategy

Kinesis charges **$0.015/hr per shard** with no free tier. Three tactics keep costs near zero:

**1. Delete the stack after each session**

```bash
bash scripts/stop_demo.sh    # billing stops immediately
bash scripts/start_demo.sh   # back up in ~30 seconds before next demo
```

**2. Peak-hours-only scheduling via EventBridge**
The Lambda poller runs only Mon–Fri 7–9am and 4–6pm ET, matching real TTC rush hours. If you leave the stream up between sessions this cuts Kinesis runtime by ~85%.

**3. Batch all records into one PutRecords call**
`producer.py` sends all ~700 vehicle positions as one batch instead of 700 individual PUTs, avoiding Kinesis's per-record billing rounding on every poll cycle.

---

## Skills Demonstrated

- **Stream processing** — Kinesis Data Streams ingestion, consumer fan-out, shard management
- **Event-driven architecture** — Decoupled producer/consumer, SNS alerting
- **Infrastructure as Code** — CloudFormation with region enforcement via `Rules` block
- **Cost optimization** — on-demand teardown, batched PutRecords, free-tier maximization, peak-hours-only scheduling
- **Data lake design** — Parquet output, date/route partitioning, Athena querying
- **Real-world data** — Live TTC GTFS-Realtime feed, Open Government Licence – Toronto

---

## Data Licence

TTC vehicle position data is published under the [Open Government Licence – Toronto](https://open.toronto.ca/open-data-licence/).

Attribution: _Contains information licensed under the Open Government Licence – Toronto._
