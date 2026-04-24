from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list

def perform_graph_analysis(spark: SparkSession, production_df):
    print("\n" + "=" * 70)
    print("4. SPARK LIBRARIES - Graph Analysis for Machine Relationships")
    print("=" * 70)
    
    high_defect_machines = production_df.filter(col("defect_rate") > 2.0).select("machine_id").distinct().collect()
    high_defect_ids = [row.machine_id for row in high_defect_machines]
    
    vertices = spark.createDataFrame([
        (101, "Machine_101", "High_Risk" if 101 in high_defect_ids else "Normal"),
        (102, "Machine_102", "High_Risk" if 102 in high_defect_ids else "Normal"),
        (103, "Machine_103", "High_Risk" if 103 in high_defect_ids else "Normal"),
        (104, "Machine_104", "High_Risk" if 104 in high_defect_ids else "Normal"),
        (1, "Morning_Shift", "Shift"),
        (2, "Afternoon_Shift", "Shift"),
        (3, "Evening_Shift", "Shift"),
    ], ["id", "name", "type"])
    
    edges = spark.createDataFrame([
        (101, 1, "operates_in"),
        (101, 2, "operates_in"),
        (101, 3, "operates_in"),
        (102, 1, "operates_in"),
        (102, 2, "operates_in"),
        (102, 3, "operates_in"),
        (103, 1, "operates_in"),
        (103, 2, "operates_in"),
        (103, 3, "operates_in"),
        (104, 1, "operates_in"),
        (104, 2, "operates_in"),
        (104, 3, "operates_in"),
    ], ["src", "dst", "relationship"])
    
    print("Production Graph Network:")
    print("Vertices (Machines and Shifts):")
    vertices.show()
    print("Edges (Machine-Shift assignments):")
    edges.show()
    
    shift_machines = edges.groupBy("dst").agg(collect_list("src").alias("machines"))
    print("Shift to Machine mapping:")
    shift_machines.show()