from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, stddev, when

def process_with_spark_core(spark: SparkSession, production_df):
    print("\n" + "=" * 70)
    print("2. SPARK CORE & SQL - Distributed Analytics on Factory Data")
    print("=" * 70)
    
    production_df.createOrReplaceTempView("factory_telemetry")
    
    print("Machine performance ranking by defect rate:")
    machine_performance = spark.sql("""
        SELECT 
            machine_id,
            AVG(temperature) as avg_temp,
            AVG(pressure) as avg_pressure,
            AVG(vibration) as avg_vibration,
            AVG(production_rate) as avg_production_rate,
            AVG(defect_rate) as avg_defect_rate,
            COUNT(*) as readings_count
        FROM factory_telemetry
        GROUP BY machine_id
        ORDER BY avg_defect_rate DESC
    """)
    
    machine_performance.show()
    
    print("\nShift performance comparison:")
    shift_performance = spark.sql("""
        SELECT 
            shift,
            AVG(production_rate) as avg_production_rate,
            AVG(defect_rate) as avg_defect_rate,
            AVG(power_consumption) as avg_power_kw,
            COUNT(DISTINCT machine_id) as machines_active
        FROM factory_telemetry
        GROUP BY shift
        ORDER BY avg_defect_rate
    """)
    
    shift_performance.show()
    
    print(f"Spark UI at http://localhost:4040 - Check Jobs tab for DAG visualization")
    
    return machine_performance, shift_performance