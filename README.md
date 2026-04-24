# 🏭 Manufacturing Analytics Platform

Apache Spark, Delta Lake, and AI-powered analytics for manufacturing data.

## Author
**Mahmoud Najmeh**  
<img src="https://avatars.githubusercontent.com/u/78208459?u=c3f9c7d6b49fc9726c5ea8bce260656bcb9654b3&v=4" width="200px" style="border-radius: 50%;">

------------------------------------------------------------------------

## 🎯 Overview

The Manufacturing Analytics Platform processes factory sensor data using:

- Apache Spark for distributed data processing
- Delta Lake for ACID transactions and time travel
- MLlib for predictive maintenance clustering
- Structured Streaming for anomaly detection
- AI assistant with web search integration

The platform analyzes manufacturing telemetry including temperature, pressure, vibration, power consumption, production rates, and defect rates across multiple machines and shifts.

### Key Capabilities

* **Real-time Processing**: Sub-second anomaly detection from streaming IoT sensors
* **Delta Lake Time Travel**: Query any historical version (versionAsOf, timestampAsOf)
* **MLlib Predictive Maintenance**: K-means clustering for machine risk assessment
* **Graph Analytics**: Machine-shift relationship mapping
* **AI Assistant**: Natural language Q&A with web search (Groq + Tavily)
* **ACID Compliance**: Schema enforcement, transaction logging, VACUUM support
* **Multiple Outputs**: Executive dashboards, warehouse exports, quality alerts

---

## ✨ Features

### Core Features

* 🏭 **Real-time sensor ingestion** from CSV and JSON streaming sources
* 🔍 **5-stage analytics pipeline**: Ingest → Process → ML → Graph → Output
* 🤖 **AI-powered assistant** with real-time web search (Groq Llama 3.3 + Tavily)
* 📊 **Delta Lake Time Travel & Versioning**:

  * `versionAsOf` - Query historical snapshots by version number
  * `timestampAsOf` - Query data as it existed at any point in time
  * `history()` - Full version log with operation metadata
  * `VACUUM` - Physical deletion of old Parquet files (GDPR compliance)
  * 4 versions tracked (0→1→2→3) with complete audit trail
* ⚙️ **MLlib predictive maintenance**: K-means clustering for risk levels (High/Medium/Low)
* 📈 **GraphX-style analysis**: Machine-shift relationships and performance mapping
* 💾 **Multiple storage formats**: Delta Lake, Parquet, CSV exports
* 📊 **Executive dashboards**: Shift performance, defect rates, OEE metrics

### Technical Features

* **Modular architecture**: 12 specialized modules with clear separation of concerns
* **Structured Streaming**: 30-second windowed anomaly detection
* **Schema enforcement**: Automatic rejection of mismatched data types
* **Comprehensive testing**: 15 passing tests with pytest
* **Professional project structure**: src/ layout with uv package manager
* **Real-time web search**: Tavily + Groq integration for up-to-date answers

---

## 🏗 Architecture

### Architecture Overview Diagram
<img width="5689" height="4439" alt="Image" src="https://github.com/user-attachments/assets/5dbb54be-0dbe-45c3-be2e-ebb469743379" />


### Delta Lake Time Travel & Versioning Flow
<img width="4873" height="3236" alt="Image" src="https://github.com/user-attachments/assets/b5c5215f-beef-4221-8142-575945daa25b" />


### AI Assistant Architecture
<img width="3909" height="2319" alt="Image" src="https://github.com/user-attachments/assets/cd697667-db04-4ce5-a79f-9eab89b70deb" />


### Complete Pipeline Data Flow
<img width="6826" height="4028" alt="Image" src="https://github.com/user-attachments/assets/9d2f3619-0690-42b7-bf16-10651175659d" />


### Project Module Structure
<img width="11442" height="1243" alt="Image" src="https://github.com/user-attachments/assets/9bb5b434-53de-49b3-a1a3-62c60f0337d7" />


### Technology Stack
<img width="3430" height="1755" alt="Image" src="https://github.com/user-attachments/assets/c8019c2a-db7d-44f8-b5f7-e245cd6da778" />

### Test Coverage Diagram
<img width="250" height="auto" alt="Image" src="https://github.com/user-attachments/assets/9c57dc7e-b306-497e-b571-febd555619a9" />


---

## 🚀 Quick Start

### Prerequisites

* Python 3.12 or higher
* [uv](https://github.com/astral-sh/uv) for dependency management
* 4GB RAM minimum (8GB recommended)
* (Optional) API keys for AI features

### Setup Steps

```bash
# 1. Clone the repository
git clone https://github.com/mahmoudnajmeh/manufacturing-analytics-spark.git
cd manufacturing_analytics_spark

# 2. Install dependencies using uv
uv sync

# 3. Create data directory and add your CSV file
mkdir -p data
# Copy production_metrics.csv to data/

# 4. (Optional) Set up API keys for AI assistant
export TAVILY_API_KEY="your-tavily-key"
export GROQ_API_KEY="your-groq-key"

# 5. Run the complete pipeline
uv run python -m manufacturing_analytics.main
```

---

## Data Format

Create `data/production_metrics.csv` with the following schema:

```csv
machine_id,temperature,pressure,vibration,power_consumption,production_rate,defect_rate,timestamp,shift
101,72.5,14.2,0.32,45.6,98.5,2.1,2026-04-21 08:00:00,Morning
102,74.1,14.5,0.28,47.2,97.8,1.9,2026-04-21 08:00:00,Morning
```

---

## 📁 Project Structure

```text
manufacturing_analytics_spark/
├── .venv/
├── chroma_db/
├── data/
│   ├── production_metrics.csv
│   └── sensor_streaming/
├── lake/
│   ├── manufacturing_delta/
│   ├── manufacturing_parquet/
│   ├── manufacturing_warehouse/
│   └── quality_alerts/
├── src/
│   └── manufacturing_analytics/
│       ├── __init__.py
│       ├── ai_assistant.py
│       ├── config.py
│       ├── data_ingestion.py
│       ├── delta_time_travel.py
│       ├── graph_analytics.py
│       ├── main.py
│       ├── ml_analytics.py
│       ├── output.py
│       ├── processing.py
│       ├── streaming.py
│       ├── storage.py
│       └── utils.py
├── tests/
│   ├── __init__.py
│   └── test_manufacturing.py
├── .gitignore
├── .python-version
├── pyproject.toml
├── README.md
└── uv.lock
```

---

## 🔧 Commands

### Run the Application

```bash
# Run full pipeline
uv run python -m manufacturing_analytics.main

# Run with AI features (requires API keys)
export TAVILY_API_KEY="your-key"
export GROQ_API_KEY="your-key"
uv run python -m manufacturing_analytics.main
```

### Testing

```bash
# Run all tests
uv run pytest tests/ -v

# Run specific test file
uv run pytest tests/test_manufacturing.py -v

# Run with coverage
uv run pytest tests/ -v --cov=src/manufacturing_analytics --cov-report=html

# Run specific test
uv run pytest tests/test_manufacturing.py::test_version_0_exists -v
```

### Delta Lake Operations

```bash
# Query version history from within Python
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, "lake/manufacturing_delta")
history = delta_table.history().show()

# Time travel to version 0
v0 = spark.read.format("delta").option("versionAsOf", 0).load("lake/manufacturing_delta")

# Run VACUUM (remove old files)
delta_table.vacuum(retentionHours=168)  # 7 days default
```
