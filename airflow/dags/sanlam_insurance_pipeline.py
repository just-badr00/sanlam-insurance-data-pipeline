"""Four-day incremental orchestration for the Sanlam insurance pipeline."""
from __future__ import annotations

import subprocess
import sys
from importlib.util import module_from_spec, spec_from_file_location
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

PROJECT_ROOT = Path("/opt/sanlam")
RAW_TABLES = (
    "CUSTOMERS", "AGENTS", "INSURANCE_PRODUCTS", "POLICIES",
    "POLICY_COVERAGES", "POLICY_STATUS_HISTORY", "PREMIUM_TRANSACTIONS",
    "CLAIMS", "CLAIM_PAYMENTS",
)


def _pipeline_jobs():
    sys.path.insert(0, str(PROJECT_ROOT))
    sys.path.insert(0, str(PROJECT_ROOT / "src"))
    module_path = PROJECT_ROOT / "src" / "pipeline_jobs.py"
    spec = spec_from_file_location("sanlam_pipeline_jobs", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load pipeline jobs from {module_path}")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@dag(
    dag_id="sanlam_insurance_incremental_pipeline",
    description="Seed Oracle changes, incrementally land raw data, and rebuild Silver/Gold/Tableau outputs.",
    start_date=datetime(2026, 8, 17),
    schedule=timedelta(days=4),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["sanlam", "insurance", "minio", "incremental"],
)
def sanlam_insurance_incremental_pipeline():
    @task
    def seed_incremental_source() -> None:
        subprocess.run([sys.executable, str(PROJECT_ROOT / "src" / "seed_insurance_oracle.py"), "--mode", "incremental", "--batch-size", "400"], check=True, cwd=PROJECT_ROOT)

    @task
    def ingest_raw_table(table_name: str) -> int:
        return _pipeline_jobs().ingest_table(table_name)

    @task
    def transform_raw_to_silver() -> None:
        _pipeline_jobs().run_silver()

    @task
    def transform_silver_to_gold() -> None:
        _pipeline_jobs().run_gold()

    @task
    def export_gold_for_tableau() -> None:
        _pipeline_jobs().export_tableau()

    seed = seed_incremental_source()
    raw_loads = [ingest_raw_table.override(task_id=f"ingest_{name.lower()}")(name) for name in RAW_TABLES]
    silver = transform_raw_to_silver()
    gold = transform_silver_to_gold()
    tableau = export_gold_for_tableau()
    seed >> raw_loads >> silver >> gold >> tableau


sanlam_insurance_incremental_pipeline()
