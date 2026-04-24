from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from manufacturing_analytics.config import SPARK_CONFIG

def create_spark_session():
    builder = SparkSession.builder \
        .master("local[*]") \
        .appName("manufacturing-intelligence-platform")
    
    for key, value in SPARK_CONFIG.items():
        builder = builder.config(key, value)
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

def print_header():
    print("\n" + "=" * 70)
    print("MANUFACTURING INTELLIGENCE PLATFORM")
    print("Apache Spark - Unified Analytics for Industry 4.0")
    print("=" * 70)
    print("\n🌐 OPEN http://localhost:4040 IN YOUR BROWSER")
    print("📊 Monitor Jobs, Stages, SQL Queries, and Streaming\n")