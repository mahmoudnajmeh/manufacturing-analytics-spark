from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, stddev, count
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

def perform_ml_analytics(spark: SparkSession, production_df):
    print("\n" + "=" * 70)
    print("3. SPARK LIBRARIES - MLlib for Predictive Maintenance")
    print("=" * 70)
    
    ml_df = production_df.groupBy("machine_id").agg(
        avg("temperature").alias("avg_temperature"),
        avg("pressure").alias("avg_pressure"),
        avg("vibration").alias("avg_vibration"),
        avg("defect_rate").alias("avg_defect_rate"),
        stddev("temperature").alias("temp_variance")
    )
    
    assembler = VectorAssembler(
        inputCols=["avg_temperature", "avg_pressure", "avg_vibration", "avg_defect_rate", "temp_variance"],
        outputCol="features"
    )
    vector_data = assembler.transform(ml_df)
    
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    scaler_model = scaler.fit(vector_data)
    scaled_data = scaler_model.transform(vector_data)
    
    kmeans = KMeans(featuresCol="scaled_features", k=3, seed=42)
    model = kmeans.fit(scaled_data)
    
    print("Machine Clustering Results (High/Medium/Low Risk):")
    result = model.transform(scaled_data)
    result.select("machine_id", "avg_defect_rate", "prediction").orderBy("avg_defect_rate", ascending=False).show()
    
    cluster_summary = result.groupBy("prediction").agg(
        avg("avg_defect_rate").alias("avg_defect"),
        avg("avg_temperature").alias("avg_temp"),
        count("machine_id").alias("machine_count")
    )
    print("\nCluster Summary:")
    cluster_summary.show()
    
    return model, result