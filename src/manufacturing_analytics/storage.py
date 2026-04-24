from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pathlib import Path
import shutil
from manufacturing_analytics.config import LAKE_DIR, PRODUCTION_SCHEMA
from pyspark.sql.functions import col, lit
from datetime import datetime

def setup_storage_layers(spark: SparkSession, production_df):
    print("\n" + "=" * 70)
    print("7. STORAGE - Delta Lake with Time Travel & Versioning")
    print("=" * 70)
    
    delta_path = str(LAKE_DIR / "manufacturing_delta")
    if Path(delta_path).exists():
        shutil.rmtree(delta_path)
    
    print("Writing sensor telemetry to Delta Lake format:")
    production_df.write.format("delta").mode("overwrite").save(delta_path)
    print(f"Production data stored at: {delta_path}")
    
    print("\n" + "=" * 70)
    print("7.1 DELTA HISTORY - Inspecting transaction log")
    print("=" * 70)
    
    delta_table = DeltaTable.forPath(spark, delta_path)
    history = delta_table.history()
    print("Operation history:")
    history.select("version", "timestamp", "operation").show(truncate=False)
    
    print("\n" + "=" * 70)
    print("7.2 APPEND - Adding new data (creates version 1)")
    print("=" * 70)
    
    new_readings = [
        (105, 69.5, 13.5, 0.24, 42.5, 99.7, 0.8, datetime(2026, 4, 22, 8, 0, 0), "Morning"),
        (105, 70.1, 13.7, 0.25, 43.1, 99.5, 0.9, datetime(2026, 4, 22, 12, 0, 0), "Afternoon"),
    ]
    new_df = spark.createDataFrame(new_readings, schema=PRODUCTION_SCHEMA)
    new_df.write.format("delta").mode("append").save(delta_path)
    
    new_history = DeltaTable.forPath(spark, delta_path).history()
    print("After append - new version created:")
    new_history.select("version", "operation").show()
    
    print("\n" + "=" * 70)
    print("7.3 UPDATE - Modifying existing data (creates version 2)")
    print("=" * 70)
    
    delta_table = DeltaTable.forPath(spark, delta_path)
    delta_table.update(
        condition=col("machine_id") == 101,
        set={"defect_rate": lit(2.0)}
    )
    print("Updated machine 101 defect rate to 2.0%")
    
    print("\n" + "=" * 70)
    print("7.4 DELETE - Removing data (creates version 3)")
    print("=" * 70)
    
    delta_table.delete(condition=col("machine_id") == 105)
    print("Deleted machine 105 records")
    
    print("\n" + "=" * 70)
    print("7.5 TIME TRAVEL - Querying historical versions")
    print("=" * 70)
    
    print("Current version (latest):")
    current = spark.read.format("delta").load(delta_path)
    current.select("machine_id", "defect_rate").distinct().show()
    
    print("Version 0 (original data):")
    version_0 = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)
    version_0.select("machine_id", "defect_rate").distinct().show()
    
    print("Version 1 (after append):")
    version_1 = spark.read.format("delta").option("versionAsOf", 1).load(delta_path)
    version_1.select("machine_id", "defect_rate").distinct().show()
    
    print("\n" + "=" * 70)
    print("7.6 SCHEMA ENFORCEMENT DEMO")
    print("=" * 70)
    
    try:
        bad_schema_df = spark.createDataFrame([
            (999, "bad_temp", 0, 0, 0, 0, 0, datetime.now(), "Bad")
        ], ["wrong_id", "wrong_temp", "wrong_pressure", "wrong_vibration", "wrong_power", "wrong_rate", "wrong_defect", "wrong_timestamp", "wrong_shift"])
        
        bad_schema_df.write.format("delta").mode("append").save(delta_path)
        print("This should not print - schema enforcement worked!")
    except Exception as e:
        print(f"✅ Schema enforcement BLOCKED bad write: {type(e).__name__}")
        print("This is GOOD - Delta rejected mismatched schema!")
    
    print("\n" + "=" * 70)
    print("7.7 DESCRIBE HISTORY - Full audit trail")
    print("=" * 70)
    
    full_history = delta_table.history()
    full_history.select("version", "timestamp", "operation").show(10, truncate=False)
    
    print("\n" + "=" * 70)
    print("7.8 VACUUM - Retention policy")
    print("=" * 70)
    
    print("Current retention: 7 days (default)")
    print("To physically delete old files:")
    print("  delta_table.vacuum()  # keeps 7 days")
    print("  delta_table.vacuum(retentionHours=0)  # aggressive cleanup")
    print("\n⚠️ After VACUUM, time travel to removed versions fails with FileNotFoundException")
    
    print("\n" + "=" * 70)
    print("7.9 VERIFY CURRENT STATE")
    print("=" * 70)
    
    final_df = spark.read.format("delta").load(delta_path)
    print(f"Final row count: {final_df.count()}")
    print("Current machines in table:")
    final_df.select("machine_id").distinct().show()
    
    parquet_path = str(LAKE_DIR / "manufacturing_parquet")
    production_df.write.mode("overwrite").parquet(parquet_path)
    print(f"\nAlso stored in Parquet format: {parquet_path}")
    
    return delta_path, parquet_path