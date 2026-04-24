from pathlib import Path
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

DATA_DIR = Path(__file__).parent.parent.parent / "data"
LAKE_DIR = Path(__file__).parent.parent.parent / "lake"
STREAMING_CHECKPOINT = str(Path(__file__).parent.parent.parent / "checkpoint")

PRODUCTION_SCHEMA = StructType([
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

SPARK_CONFIG = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.executor.memory": "2g",
    "spark.driver.memory": "2g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
}