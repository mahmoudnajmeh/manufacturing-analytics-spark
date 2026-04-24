from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def diff_versions(spark: SparkSession, delta_path: str, version_a: int, version_b: int) -> DataFrame:
    df_a = spark.read.format("delta").option("versionAsOf", version_a).load(delta_path)
    df_b = spark.read.format("delta").option("versionAsOf", version_b).load(delta_path)
    
    common_columns = set(df_a.columns) & set(df_b.columns)
    df_a_common = df_a.select(*common_columns)
    df_b_common = df_b.select(*common_columns)
    
    return df_b_common.exceptAll(df_a_common)

def summarize_history(spark: SparkSession, delta_path: str) -> dict:
    delta_table = DeltaTable.forPath(spark, delta_path)
    history_df = delta_table.history()
    operation_counts = history_df.groupBy("operation").count().collect()
    return {row["operation"]: row["count"] for row in operation_counts}

def restore_to_version(spark: SparkSession, delta_path: str, target_version: int) -> None:
    df_target = spark.read.format("delta").option("versionAsOf", target_version).load(delta_path)
    df_target.write.format("delta").mode("overwrite").save(delta_path)
    logger.info(f"Restored to version {target_version}")

def get_version_count(spark: SparkSession, delta_path: str) -> int:
    delta_table = DeltaTable.forPath(spark, delta_path)
    return delta_table.history().count()

def time_travel_demo(spark: SparkSession, delta_path: str):
    print("\n" + "=" * 70)
    print("TIME TRAVEL DEMO - Delta Lake Versioning")
    print("=" * 70)
    
    versions = get_version_count(spark, delta_path)
    print(f"Total versions in table: {versions}")
    
    print("\n1. DIFF BETWEEN VERSIONS 0 and 1:")
    try:
        diff = diff_versions(spark, delta_path, 0, 1)
        print(f"Rows changed between v0 and v1: {diff.count()}")
        diff.show(5)
    except Exception as e:
        print(f"Cannot diff (need multiple versions): {e}")
    
    print("\n2. HISTORY SUMMARY:")
    history_summary = summarize_history(spark, delta_path)
    print(f"Operation counts: {history_summary}")
    
    print("\n3. CURRENT VERSION:")
    current = spark.read.format("delta").load(delta_path)
    print(f"Row count: {current.count()}")
    
    if versions >= 1:
        print("\n4. VERSION 0 (original):")
        v0 = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)
        print(f"Row count in version 0: {v0.count()}")