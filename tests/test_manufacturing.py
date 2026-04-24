import shutil
from pathlib import Path
import pytest
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from manufacturing_analytics.utils import create_spark_session
from manufacturing_analytics.storage import setup_storage_layers
from manufacturing_analytics.data_ingestion import ingest_production_data
from manufacturing_analytics.config import PRODUCTION_SCHEMA
from manufacturing_analytics.delta_time_travel import diff_versions, summarize_history

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
LAKE_DIR = PROJECT_ROOT / "lake"
DELTA_PATH = str(LAKE_DIR / "manufacturing_delta")


@pytest.fixture(scope="module")
def spark():
    session = create_spark_session()
    yield session
    session.stop()


@pytest.fixture(scope="module")
def setup_delta_table(spark):
    production_df = ingest_production_data(spark)
    delta_path, parquet_path = setup_storage_layers(spark, production_df)
    
    yield delta_path
    
    if Path(delta_path).exists():
        shutil.rmtree(delta_path)


def test_csv_exists():
    assert (DATA_DIR / "production_metrics.csv").is_file()


def test_csv_has_12_rows(spark):
    df = spark.read.option("header", "true").schema(PRODUCTION_SCHEMA).csv(
        str(DATA_DIR / "production_metrics.csv")
    )
    assert df.count() == 12


def test_delta_table_created(setup_delta_table):
    assert Path(DELTA_PATH).exists()
    assert Path(DELTA_PATH + "/_delta_log").exists()


def test_current_version_has_rows(setup_delta_table, spark):
    df = spark.read.format("delta").load(DELTA_PATH)
    assert df.count() > 0


def test_version_0_exists(spark):
    df = spark.read.format("delta").option("versionAsOf", 0).load(DELTA_PATH)
    assert df.count() == 12


def test_version_1_exists(spark):
    df = spark.read.format("delta").option("versionAsOf", 1).load(DELTA_PATH)
    assert df.count() == 14


def test_version_2_machine_101_updated(spark):
    df = spark.read.format("delta").option("versionAsOf", 2).load(DELTA_PATH)
    machine_101 = df.filter(F.col("machine_id") == 101).first()
    assert machine_101.defect_rate == 2.0


def test_version_3_machine_105_deleted(spark):
    df = spark.read.format("delta").option("versionAsOf", 3).load(DELTA_PATH)
    machines = df.select("machine_id").distinct().collect()
    machine_ids = [row.machine_id for row in machines]
    assert 105 not in machine_ids
    assert df.count() == 12


def test_history_has_4_versions(spark):
    delta_table = DeltaTable.forPath(spark, DELTA_PATH)
    history = delta_table.history()
    assert history.count() >= 4


def test_history_has_write_update_delete(spark):
    delta_table = DeltaTable.forPath(spark, DELTA_PATH)
    history = delta_table.history()
    operations = [row.operation for row in history.select("operation").collect()]
    assert "WRITE" in operations
    assert "UPDATE" in operations
    assert "DELETE" in operations


def test_schema_enforcement_blocks_bad_data(setup_delta_table, spark):
    bad_df = spark.createDataFrame([
        (999, "bad", 0, 0, 0, 0, 0, "2026-04-24", "Bad")
    ], ["wrong_id", "wrong_temp", "wrong_pressure", "wrong_vibration",
        "wrong_power", "wrong_rate", "wrong_defect", "wrong_timestamp", "wrong_shift"])
    
    with pytest.raises(Exception):
        bad_df.write.format("delta").mode("append").save(DELTA_PATH)


def test_time_travel_back_to_version_0(spark):
    v0 = spark.read.format("delta").option("versionAsOf", 0).load(DELTA_PATH)
    v0_machines = v0.select("machine_id").distinct().collect()
    v0_ids = [row.machine_id for row in v0_machines]
    assert 101 in v0_ids
    assert 102 in v0_ids
    assert 103 in v0_ids
    assert 104 in v0_ids
    assert 105 not in v0_ids


def test_time_travel_to_version_1_shows_machine_105(spark):
    v1 = spark.read.format("delta").option("versionAsOf", 1).load(DELTA_PATH)
    machines = v1.select("machine_id").distinct().collect()
    machine_ids = [row.machine_id for row in machines]
    assert 105 in machine_ids


def test_diff_versions_v0_to_v1(spark):
    diff = diff_versions(spark, DELTA_PATH, 0, 1)
    assert diff.count() == 2
    machines = diff.select("machine_id").distinct().collect()
    assert machines[0].machine_id == 105


def test_summarize_history_returns_dict(spark):
    summary = summarize_history(spark, DELTA_PATH)
    assert isinstance(summary, dict)
    assert "WRITE" in summary
    assert "UPDATE" in summary
    assert "DELETE" in summary