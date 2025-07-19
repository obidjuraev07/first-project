-- PostgreSQL to ClickHouse Migration Script
-- For large tables with 50M+ daily records

-- Step 1: Create ClickHouse table with optimized structure
CREATE TABLE taxonomy_clicstream.traffic_daily_staging
(
    `msisdn` String,
    `source_type` Enum8('web' = 0, 'app' = 1),
    `source_name` String,
    `usage_count` UInt32,
    `gender_id` UInt8,
    `age_group_id` UInt8,
    `region_id` UInt32,
    `district_id` UInt32,
    `pdate` Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(pdate)
ORDER BY (source_type, pdate, region_id, district_id)
SETTINGS index_granularity = 8192;

-- Step 2: Create PostgreSQL table engines in ClickHouse for source tables
CREATE TABLE ch_web_traffic_daily AS postgresql(
    'your_postgres_host:5432', 
    'your_database', 
    'web_traffic_daily', 
    'username', 
    'password',
    'clickstream'
);

CREATE TABLE ch_app_traffic_daily AS postgresql(
    'your_postgres_host:5432', 
    'your_database', 
    'app_traffic_daily', 
    'username', 
    'password',
    'clickstream'
);

-- Step 3: Migrate web traffic data in batches
-- Method A: Date-based batching (recommended for large datasets)
INSERT INTO taxonomy_clicstream.traffic_daily_staging
SELECT 
    msisdn,
    0 as source_type, -- 'web'
    domain as source_name,
    count as usage_count,
    gender_ind as gender_id,
    age_ind as age_group_id,
    region_id,
    0 as district_id, -- default value
    pdate::Date as pdate
FROM ch_web_traffic_daily
WHERE pdate >= '2024-01-01' AND pdate < '2024-01-02'  -- Process one day at a time
SETTINGS max_insert_threads = 4, max_insert_block_size = 1000000;

-- Step 4: Migrate app traffic data in batches
INSERT INTO taxonomy_clicstream.traffic_daily_staging
SELECT 
    msisdn,
    1 as source_type, -- 'app'
    app_name as source_name,
    count as usage_count,
    gender_ind as gender_id,
    age_ind as age_group_id,
    region_id,
    0 as district_id, -- default value
    pdate::Date as pdate
FROM ch_app_traffic_daily
WHERE pdate >= '2024-01-01' AND pdate < '2024-01-02'  -- Process one day at a time
SETTINGS max_insert_threads = 4, max_insert_block_size = 1000000;

-- Step 5: Optimize the table after migration
OPTIMIZE TABLE taxonomy_clicstream.traffic_daily_staging FINAL;

-- Step 6: Rename staging to final table
RENAME TABLE taxonomy_clicstream.traffic_daily_staging TO taxonomy_clicstream.traffic_daily;