from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from pathlib import Path
from manufacturing_analytics.config import DATA_DIR

def setup_streaming(spark: SparkSession):
    print("\n" + "=" * 70)
    print("5. SPARK LIBRARIES - Structured Streaming for Real-time Sensors")
    print("=" * 70)
    
    streaming_path = str(DATA_DIR / "sensor_streaming")
    Path(streaming_path).mkdir(exist_ok=True)
    
    print(f"Monitoring real-time sensor data in: {streaming_path}")
    print("Check Spark UI Streaming tab for live processing")
    
    streaming_schema = StructType([
        StructField("machine_id", IntegerType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("vibration", DoubleType(), True),
        StructField("power_consumption", DoubleType(), True),
        StructField("production_rate", DoubleType(), True),
        StructField("defect_rate", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("shift", StringType(), True)
    ])
    
    streaming_df = spark.readStream.schema(streaming_schema).json(streaming_path)
    
    aggregated_stream = streaming_df.groupBy(
        window(col("timestamp"), "30 seconds"),
        col("machine_id"),
        col("shift")
    ).agg(
        avg("defect_rate").alias("avg_defect_rate"),
        avg("temperature").alias("avg_temperature"),
        avg("vibration").alias("avg_vibration"),
        count("machine_id").alias("readings_count")
    ).filter(col("avg_defect_rate") > 1.8)
    
    print("Streaming Query: Real-time anomaly detection")
    print("Alert conditions: avg_defect_rate > 1.8%")
    
    query = aggregated_stream.writeStream.outputMode("complete").format("console").queryName("sensor_analytics").start()
    
    return query