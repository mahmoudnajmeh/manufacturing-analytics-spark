from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when, count
from manufacturing_analytics.config import LAKE_DIR

def generate_outputs(spark: SparkSession, production_df):
    print("\n" + "=" * 70)
    print("8. OUTPUT / CONSUMPTION - Manufacturing Intelligence")
    print("=" * 70)
    
    dashboard_df = production_df.groupBy("shift", "machine_id").agg(
        avg("production_rate").alias("OEE_Production_Rate"),
        avg("defect_rate").alias("Quality_Defect_Rate"),
        avg("power_consumption").alias("Energy_Power_KW"),
        count("timestamp").alias("Total_Readings")
    )
    
    print("1. Executive Dashboard - Shift Performance:")
    dashboard_df.orderBy("Quality_Defect_Rate").show()
    
    dashboard_df.createOrReplaceTempView("manufacturing_kpis")
    adhoc = spark.sql("""
        SELECT 
            shift,
            ROUND(AVG(OEE_Production_Rate), 2) as avg_production,
            ROUND(AVG(Quality_Defect_Rate), 2) as avg_defect,
            ROUND(AVG(Energy_Power_KW), 2) as avg_power,
            CASE 
                WHEN AVG(Quality_Defect_Rate) < 1.5 THEN 'Excellent'
                WHEN AVG(Quality_Defect_Rate) < 2.0 THEN 'Good'
                ELSE 'Needs Improvement'
            END as performance_rating
        FROM manufacturing_kpis
        GROUP BY shift
        ORDER BY avg_defect
    """)
    
    print("\n2. Ad-hoc Analytics - Shift Performance Ratings:")
    adhoc.show()
    
    print("\n3. Predictive Maintenance Alerts:")
    high_risk = production_df.groupBy("machine_id").agg(
        avg("defect_rate").alias("defect_rate"),
        avg("vibration").alias("vibration"),
        avg("temperature").alias("temperature")
    ).filter(
        (col("defect_rate") > 1.9) | (col("vibration") > 0.34) | (col("temperature") > 74.5)
    )
    
    high_risk.withColumn(
        "maintenance_priority",
        when(col("defect_rate") > 2.2, "URGENT")
        .when(col("defect_rate") > 1.9, "High")
        .otherwise("Medium")
    ).show()
    
    print("\n4. Data Warehouse Export:")
    production_df.write.mode("overwrite").parquet(str(LAKE_DIR / "manufacturing_warehouse"))
    print(f"Data exported to warehouse: {LAKE_DIR / 'manufacturing_warehouse'}")
    
    print("\n5. Downstream Systems Integration:")
    production_df.filter(col("defect_rate") > 2.0).coalesce(1).write.mode("overwrite").csv(
        str(LAKE_DIR / "quality_alerts"), header=True
    )
    print("Quality alerts exported for MES system integration")