-- =============================================================================
-- analysis/athena_queries.sql
-- TTC Real-Time Streaming Pipeline — Long-term Pattern Analysis
--
-- Run these in the AWS Athena console against the S3 data lake.
-- The table must first be registered using the CREATE EXTERNAL TABLE
-- statement below. Athena charges $5 per TB scanned — partitioned
-- Parquet files keep this to pennies for portfolio-scale data.
-- =============================================================================

--Register the table (run once after first data lands in S3)
CREATE EXTERNAL TABLE IF NOT EXISTS ttc_vehicle_positions (
    vehicle_id              STRING,
    route_id                STRING,
    latitude                DOUBLE,
    longitude               DOUBLE,
    speed_kmh               DOUBLE,
    timestamp               BIGINT,
    ingested_at             BIGINT,
    processing_timestamp    STRING,
    is_idle                 BOOLEAN,
    idle_duration_seconds   BIGINT
)
PARTITIONED BY (
    year    INT,
    month   INT,
    day     INT,
    route   STRING
)
STORED AS PARQUET
LOCATION 's3://{S3_BUCKET_NAME}/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- Load partition metadata so Athena can discover existing files
MSCK REPAIR TABLE ttc_vehicle_positions;

-- =============================================================================
-- ANALYSIS QUERIES
-- =============================================================================
 
-- -----------------------------------------------------------------------------
-- Q1: Top 10 routes with the most idle events
-- Use case: identify which routes have the worst bunching or traffic problems.
-- -----------------------------------------------------------------------------
SELECT
    route_id,
    COUNT(*) AS idle_events,
    ROUND(AVG(idle_duration_seconds) / 60, 1) AS avg_idle_minutes,
    ROUND(MAX(idle_duration_seconds) / 60, 1) AS max_idle_minutes
FROM ttc_vehicle_positions
WHERE is_idle = true
GROUP BY route_id
ORDER BY
    idle_events DESC
LIMIT 10;
    



-- -----------------------------------------------------------------------------
-- Q2: Idle events by hour of day (Toronto ET = UTC-4 in summer)
-- Use case: understand when during the day idle events peak.
-- -----------------------------------------------------------------------------
SELECT
    ((timestamp / 3600)) % 24 as hours_of_day,
    COUNT(*) AS idle_events
FROM 
    ttc_vehicle_positions
WHERE 
    is_idle = TRUE
GROUP BY ((timestamp / 3600)) % 24
ORDER BY hours_of_day;


-- -----------------------------------------------------------------------------
-- Q3: Average vehicle speed by route
-- Use case: compare route performance; slow average = congestion.
-- -----------------------------------------------------------------------------
SELECT
    route_id,
    ROUND(MAX(speed_kmh), 1) AS max_speed_kmh,
    ROUND(MIN(speed_kmh), 1) AS min_speed_kmh,
    ROUND(AVG(speed_kmh), 1) AS avg_speed_kmh
FROM
    ttc_vehicle_positions
WHERE 
    speed_kmh > 0
GROUP BY
    route_id
ORDER BY
    avg_speed_kmh DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- Q4: Daily idle event count trend
-- Use case: see if idle events are increasing week over week.
-- -----------------------------------------------------------------------------
SELECT 
    year,
    month,
    day,
    COUNT(*) AS idle_events
FROM ttc_vehicle_positions
WHERE
    is_idle = TRUE
GROUP BY
    year, month, day
ORDER BY
    year, month, day;