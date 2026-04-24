from pyspark.sql import SparkSession
from manufacturing_analytics.config import DATA_DIR, PRODUCTION_SCHEMA

def ingest_production_data(spark: SparkSession):
    print("=" * 70)
    print("1. DATA SOURCES - Ingesting Manufacturing Telemetry")
    print("=" * 70)
    
    csv_path = str(DATA_DIR / "production_metrics.csv")
    production_df = spark.read.option("header", "true").schema(PRODUCTION_SCHEMA).csv(csv_path)
    
    print(f"From FILES (CSV) - Real-time machine telemetry:")
    production_df.show(10)
    print(f"Total sensor readings: {production_df.count()}")
    print(f"Machines monitored: {production_df.select('machine_id').distinct().count()}")
    
    return production_df