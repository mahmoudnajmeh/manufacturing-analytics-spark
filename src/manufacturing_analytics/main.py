from manufacturing_analytics.utils import create_spark_session, print_header
from manufacturing_analytics.data_ingestion import ingest_production_data
from manufacturing_analytics.processing import process_with_spark_core
from manufacturing_analytics.ml_analytics import perform_ml_analytics
from manufacturing_analytics.graph_analytics import perform_graph_analysis
from manufacturing_analytics.streaming import setup_streaming
from manufacturing_analytics.storage import setup_storage_layers
from manufacturing_analytics.output import generate_outputs
from manufacturing_analytics.ai_assistant import interactive_ai_session
import time

def main():
    spark = create_spark_session()
    print_header()
    
    production_df = ingest_production_data(spark)
    
    machine_performance, shift_performance = process_with_spark_core(spark, production_df)
    
    ml_model, ml_results = perform_ml_analytics(spark, production_df)
    
    perform_graph_analysis(spark, production_df)
    
    streaming_query = setup_streaming(spark)
    
    delta_path, parquet_path = setup_storage_layers(spark, production_df)
    
    generate_outputs(spark, production_df)
    interactive_ai_session(spark, production_df)
    
    print("\n" + "=" * 70)
    print("✅ MANUFACTURING PIPELINE DEPLOYED")
    print("=" * 70)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n⏹️  Shutting down streaming query...")
        if streaming_query:
            streaming_query.stop()
        print("⏹️  Stopping Spark session...")
        spark.stop()
        print("✅ Manufacturing Intelligence Platform stopped")

if __name__ == "__main__":
    main()