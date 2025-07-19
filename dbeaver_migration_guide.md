# DBeaver-Based PostgreSQL to ClickHouse Migration Guide

## Prerequisites
1. DBeaver Universal Database Tool installed
2. PostgreSQL and ClickHouse JDBC drivers configured in DBeaver
3. Network connectivity between PostgreSQL and ClickHouse

## Step 1: Configure Database Connections in DBeaver

### PostgreSQL Connection
- Host: your_postgres_host
- Port: 5432
- Database: your_database
- Username/Password: your_credentials
- Test connection to ensure connectivity

### ClickHouse Connection
- Host: your_clickhouse_host  
- Port: 8123 (HTTP) or 9000 (Native)
- Database: taxonomy_clicstream
- Username/Password: your_credentials
- Driver: ClickHouse JDBC driver

## Step 2: Create Target Table in ClickHouse

Execute this script in ClickHouse connection:

```sql
CREATE TABLE taxonomy_clicstream.traffic_daily
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
```

## Step 3: Migration Options in DBeaver

### Option A: Using Data Transfer Tool (Recommended for smaller batches)

1. Right-click on PostgreSQL table → Export Data
2. Choose "Database" as target
3. Select ClickHouse connection as target
4. Map columns appropriately:
   - msisdn → msisdn
   - domain/app_name → source_name
   - count → usage_count
   - gender_ind → gender_id
   - age_ind → age_group_id
   - region_id → region_id
   - pdate → pdate
5. Set batch size to 100,000 rows
6. Configure additional mappings for source_type and district_id

### Option B: Manual SQL Execution (Better control)

Execute these scripts one by one in ClickHouse:

#### For Web Traffic Data:
```sql
-- Create temporary view for web data
CREATE VIEW temp_web_view AS
SELECT 
    msisdn,
    0 as source_type,
    domain as source_name,
    count as usage_count,
    gender_ind as gender_id,
    age_ind as age_group_id,
    region_id,
    0 as district_id,
    pdate::Date as pdate
FROM postgresql('your_host:5432', 'your_db', 'web_traffic_daily', 'user', 'pass', 'clickstream')
WHERE pdate >= '2024-01-01' AND pdate < '2024-01-02';

-- Insert data in batches
INSERT INTO taxonomy_clicstream.traffic_daily 
SELECT * FROM temp_web_view
SETTINGS max_insert_threads = 4, max_insert_block_size = 1000000;

-- Drop temporary view
DROP VIEW temp_web_view;
```

#### For App Traffic Data:
```sql
-- Create temporary view for app data
CREATE VIEW temp_app_view AS
SELECT 
    msisdn,
    1 as source_type,
    app_name as source_name,
    count as usage_count,
    gender_ind as gender_id,
    age_ind as age_group_id,
    region_id,
    0 as district_id,
    pdate::Date as pdate
FROM postgresql('your_host:5432', 'your_db', 'app_traffic_daily', 'user', 'pass', 'clickstream')
WHERE pdate >= '2024-01-01' AND pdate < '2024-01-02';

-- Insert data in batches
INSERT INTO taxonomy_clicstream.traffic_daily 
SELECT * FROM temp_app_view
SETTINGS max_insert_threads = 4, max_insert_block_size = 1000000;

-- Drop temporary view
DROP VIEW temp_app_view;
```

## Step 4: Batch Processing Script for Large Data

Create this script to run in DBeaver's SQL console:

```sql
-- Batch processing for multiple date ranges
SET max_insert_threads = 8;
SET max_insert_block_size = 1000000;
SET max_memory_usage = 10000000000; -- 10GB

-- Process data month by month to avoid memory issues
-- January 2024
INSERT INTO taxonomy_clicstream.traffic_daily 
SELECT msisdn, 0, domain, count, gender_ind, age_ind, region_id, 0, pdate::Date
FROM postgresql('host:5432', 'db', 'web_traffic_daily', 'user', 'pass', 'clickstream')
WHERE pdate >= '2024-01-01' AND pdate < '2024-02-01';

INSERT INTO taxonomy_clicstream.traffic_daily 
SELECT msisdn, 1, app_name, count, gender_ind, age_ind, region_id, 0, pdate::Date
FROM postgresql('host:5432', 'db', 'app_traffic_daily', 'user', 'pass', 'clickstream')
WHERE pdate >= '2024-01-01' AND pdate < '2024-02-01';

-- Repeat for other months...
```

## Step 5: Optimization and Validation

### Optimize Table After Migration:
```sql
OPTIMIZE TABLE taxonomy_clicstream.traffic_daily FINAL;
```

### Validate Data Count:
```sql
-- Check total records
SELECT count() FROM taxonomy_clicstream.traffic_daily;

-- Check by source type
SELECT source_type, count() FROM taxonomy_clicstream.traffic_daily GROUP BY source_type;

-- Check date range
SELECT min(pdate), max(pdate) FROM taxonomy_clicstream.traffic_daily;
```

## Performance Tips for DBeaver Migration

1. **Increase Java Heap Size**: In dbeaver.ini, set -Xmx4g or higher
2. **Connection Pool**: Configure connection pooling for better performance
3. **Batch Size**: Use 100K-1M batch sizes for optimal performance
4. **Network**: Ensure good network connectivity between databases
5. **Monitoring**: Monitor both source and target database performance

## Troubleshooting Common Issues

### Memory Issues:
- Reduce batch size
- Increase DBeaver memory allocation
- Process data in smaller date ranges

### Connection Timeouts:
- Increase connection timeout settings
- Use connection pooling
- Process data in smaller chunks

### Data Type Mismatches:
- Explicitly cast data types in SELECT statements
- Handle NULL values appropriately
- Validate enum mappings

## Alternative: Using DBeaver's Database Compare Feature

1. Right-click on database → Compare with...
2. Select target ClickHouse database
3. Configure comparison settings
4. Generate and execute migration scripts

This approach provides better control over the migration process and handles schema differences automatically.