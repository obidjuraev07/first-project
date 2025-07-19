#!/usr/bin/env python3
"""
Advanced PostgreSQL to ClickHouse Migration using PySpark
Optimized for 50M+ daily records with parallel processing
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, date_format
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create optimized Spark session for large data migration"""
    return SparkSession.builder \
        .appName("PostgreSQL-to-ClickHouse-Migration") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.shuffle.partitions", "400") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .getOrCreate()

def migrate_web_traffic(spark, postgres_config, clickhouse_config, date_partition):
    """Migrate web traffic data for a specific date partition"""
    logger.info(f"Processing web traffic data for {date_partition}")
    
    # Read from PostgreSQL with partitioning
    web_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}") \
        .option("dbtable", f"(SELECT * FROM clickstream.web_traffic_daily WHERE pdate = '{date_partition}') as web_data") \
        .option("user", postgres_config['username']) \
        .option("password", postgres_config['password']) \
        .option("fetchsize", 100000) \
        .option("numPartitions", 20) \
        .option("partitionColumn", "region_id") \
        .option("lowerBound", 1) \
        .option("upperBound", 100) \
        .load()
    
    # Transform data to match ClickHouse schema
    transformed_df = web_df.select(
        col("msisdn"),
        lit(0).alias("source_type"),  # 'web' = 0
        col("domain").alias("source_name"),
        col("count").cast(IntegerType()).alias("usage_count"),
        col("gender_ind").cast(ByteType()).alias("gender_id"),
        col("age_ind").cast(ByteType()).alias("age_group_id"),
        col("region_id").cast(IntegerType()),
        lit(0).cast(IntegerType()).alias("district_id"),  # default value
        col("pdate").cast(DateType())
    ).filter(col("msisdn").isNotNull())
    
    # Write to ClickHouse
    write_to_clickhouse(transformed_df, clickhouse_config, "web")
    
    return transformed_df.count()

def migrate_app_traffic(spark, postgres_config, clickhouse_config, date_partition):
    """Migrate app traffic data for a specific date partition"""
    logger.info(f"Processing app traffic data for {date_partition}")
    
    # Read from PostgreSQL with partitioning
    app_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}") \
        .option("dbtable", f"(SELECT * FROM clickstream.app_traffic_daily WHERE pdate = '{date_partition}') as app_data") \
        .option("user", postgres_config['username']) \
        .option("password", postgres_config['password']) \
        .option("fetchsize", 100000) \
        .option("numPartitions", 20) \
        .option("partitionColumn", "region_id") \
        .option("lowerBound", 1) \
        .option("upperBound", 100) \
        .load()
    
    # Transform data to match ClickHouse schema
    transformed_df = app_df.select(
        col("msisdn"),
        lit(1).alias("source_type"),  # 'app' = 1
        col("app_name").alias("source_name"),
        col("count").cast(IntegerType()).alias("usage_count"),
        col("gender_ind").cast(ByteType()).alias("gender_id"),
        col("age_ind").cast(ByteType()).alias("age_group_id"),
        col("region_id").cast(IntegerType()),
        lit(0).cast(IntegerType()).alias("district_id"),  # default value
        col("pdate").cast(DateType())
    ).filter(col("msisdn").isNotNull())
    
    # Write to ClickHouse
    write_to_clickhouse(transformed_df, clickhouse_config, "app")
    
    return transformed_df.count()

def write_to_clickhouse(df, clickhouse_config, source_type):
    """Write DataFrame to ClickHouse with optimized settings"""
    logger.info(f"Writing {source_type} data to ClickHouse")
    
    # Repartition data for optimal ClickHouse ingestion
    df_optimized = df.repartition(50, col("pdate"), col("region_id"))
    
    # Write to ClickHouse using JDBC
    df_optimized.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", f"jdbc:clickhouse://{clickhouse_config['host']}:{clickhouse_config['port']}/{clickhouse_config['database']}") \
        .option("dbtable", "taxonomy_clicstream.traffic_daily") \
        .option("user", clickhouse_config['username']) \
        .option("password", clickhouse_config['password']) \
        .option("batchsize", 100000) \
        .option("rewriteBatchedStatements", "true") \
        .option("socket_timeout", "300000") \
        .option("max_insert_threads", "8") \
        .save()

def main():
    """Main migration function"""
    # Configuration
    postgres_config = {
        'host': 'your_postgres_host',
        'port': 5432,
        'database': 'your_database',
        'username': 'your_username',
        'password': 'your_password'
    }
    
    clickhouse_config = {
        'host': 'your_clickhouse_host',
        'port': 8123,
        'database': 'taxonomy_clicstream',
        'username': 'your_username',
        'password': 'your_password'
    }
    
    # Date range for migration (adjust as needed)
    date_partitions = [
        '2024-01-01', '2024-01-02', '2024-01-03'
        # Add more dates as needed
    ]
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        total_web_records = 0
        total_app_records = 0
        
        for date_partition in date_partitions:
            logger.info(f"Processing partition: {date_partition}")
            
            # Migrate web traffic
            web_count = migrate_web_traffic(spark, postgres_config, clickhouse_config, date_partition)
            total_web_records += web_count
            logger.info(f"Migrated {web_count} web traffic records for {date_partition}")
            
            # Migrate app traffic
            app_count = migrate_app_traffic(spark, postgres_config, clickhouse_config, date_partition)
            total_app_records += app_count
            logger.info(f"Migrated {app_count} app traffic records for {date_partition}")
        
        logger.info(f"Migration completed successfully!")
        logger.info(f"Total web records migrated: {total_web_records}")
        logger.info(f"Total app records migrated: {total_app_records}")
        logger.info(f"Total records migrated: {total_web_records + total_app_records}")
        
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()