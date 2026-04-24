from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pathlib import Path
import os
import re
from pyspark.sql.functions import avg
from manufacturing_analytics.delta_time_travel import diff_versions
from manufacturing_analytics.utils import create_spark_session
from manufacturing_analytics.data_ingestion import ingest_production_data

try:
    from tavily import TavilyClient
    from groq import Groq
    SEARCH_AVAILABLE = True
except ImportError:
    SEARCH_AVAILABLE = False
    print("⚠️ Install: uv add tavily-python groq")

class ManufacturingAIAssistant:
    def __init__(self, spark: SparkSession, production_df, tavily_key: str = None, groq_key: str = None):
        self.spark = spark
        self.df = production_df
        self.delta_path = str(Path(__file__).parent / "lake" / "manufacturing_delta")
        self.df.createOrReplaceTempView("manufacturing")
        
        self.use_search = False
        if tavily_key and groq_key and SEARCH_AVAILABLE:
            self.tavily = TavilyClient(api_key=tavily_key)
            self.groq = Groq(api_key=groq_key)
            self.use_search = True
            print("✅ Real-time web search enabled (Tavily + Groq)")
        else:
            print("⚠️ No search keys. Set TAVILY_API_KEY and GROQ_API_KEY for real-time answers")
    
    def _search_web(self, question: str) -> str:
        try:
            search_result = self.tavily.search(question, max_results=3)
            context = "\n".join([f"- {r['content']}" for r in search_result['results']])
            
            response = self.groq.chat.completions.create(
                messages=[
                    {"role": "system", "content": f"You are a helpful assistant. Answer based on this real-time data:\n{context}"},
                    {"role": "user", "content": question}
                ],
                model="llama-3.3-70b-versatile",
                temperature=0.7,
                max_tokens=500
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"Search error: {e}"
    
    def _handle_manufacturing_query(self, question: str) -> str:
        q = question.lower()
        
        if "history" in q or "version" in q:
            try:
                delta_table = DeltaTable.forPath(self.spark, self.delta_path)
                history = delta_table.history().select("version", "timestamp", "operation").toPandas()
                return f"Version history:\n{history.to_string(index=False)}"
            except:
                return "Delta table not found."
        
        if "diff" in q or "what changed" in q:
            try:
                from delta_time_travel import diff_versions
                diff = diff_versions(self.spark, self.delta_path, 0, 1)
                result_str = diff.limit(10).toPandas().to_string(index=False)
                return f"Changes between version 0 and 1:\n{result_str}"
            except:
                return "Run storage layer first to create versions"
        
        if "restore" in q or "rollback" in q:
            return "To restore: restore_to_version(spark, delta_path, target_version)"
        
        if "time travel" in q or "previous version" in q:
            return "To time travel: spark.read.format('delta').option('versionAsOf', 0).load('path')"
        
        if "vacuum" in q:
            return "VACUUM removes old Parquet files. Default retention: 7 days."
        
        if any(word in q for word in ['defect', 'temperature', 'pressure', 'machine']):
            try:
                result = self.df.agg(avg("defect_rate")).collect()[0][0]
                return f"Average defect rate is {result:.2f}%"
            except:
                return None
        return None
    
    def answer_question(self, question: str) -> str:
        print(f"🔍 Processing: {question}")
        
        manufacturing_answer = self._handle_manufacturing_query(question)
        if manufacturing_answer:
            return manufacturing_answer
        
        if self.use_search:
            return self._search_web(question)
        else:
            return "Set TAVILY_API_KEY and GROQ_API_KEY for real-time answers about weather, news, or any topic!"

def interactive_ai_session(spark, production_df):
    print("\n" + "=" * 70)
    print("🤖 AI ASSISTANT")
    print("=" * 70)
    print("Ask ANY question - manufacturing data OR real-time info")
    print("Examples:")
    print("  - Weather in Berlin today")
    print("  - What is time travel in Delta Lake?")
    print("  - Latest news about AI")
    print("  - Average defect rate")
    print("  - What changed between versions?")
    print("  - Show version history")
    print("\nType 'exit' to quit\n")
    
    tavily_key = os.environ.get("TAVILY_API_KEY")
    groq_key = os.environ.get("GROQ_API_KEY")
    
    if not tavily_key or not groq_key:
        print("⚠️ Missing API keys. Get free keys from:")
        print("   Tavily: https://tavily.com")
        print("   Groq: https://console.groq.com")
        print("\nThen run:")
        print("   export TAVILY_API_KEY='your-key'")
        print("   export GROQ_API_KEY='your-key'\n")
    
    assistant = ManufacturingAIAssistant(spark, production_df, tavily_key, groq_key)
    
    try:
        while True:
            question = input("🔍 Ask ANYTHING: ").strip()
            if question.lower() == 'exit':
                print("👋 Goodbye!")
                break
            if question:
                print("\n🤖 Searching web and thinking...")
                answer = assistant.answer_question(question)
                print(f"\n🤖 {answer}\n")
    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")

if __name__ == "__main__":  
    spark = create_spark_session()
    production_df = ingest_production_data(spark)
    interactive_ai_session(spark, production_df)