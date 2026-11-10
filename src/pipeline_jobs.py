"""Runnable jobs used by the Airflow DAG and based on the final notebooks.

Keep the transformation logic in normal Python modules rather than executing
notebooks in production.  The notebooks remain the learning/exploration
artifacts; this module is the repeatable operational implementation.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import boto3
import pandas as pd
from botocore.client import Config
from dotenv import load_dotenv
from pyspark.sql import DataFrame, Window, functions as F

try:
    from .oracle_db_connection import get_oracle_connection
    from .spark_minio import get_spark_session
except ImportError:
    # Airflow loads this file directly from the mounted src directory.
    from oracle_db_connection import get_oracle_connection
    from spark_minio import get_spark_session


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_TABLES = (
    "CUSTOMERS", "AGENTS", "INSURANCE_PRODUCTS", "POLICIES",
    "POLICY_COVERAGES", "POLICY_STATUS_HISTORY", "PREMIUM_TRANSACTIONS",
    "CLAIMS", "CLAIM_PAYMENTS",
)

TABLE_CONFIG = {
    "customers": {"pk": "customer_id", "dates": ["date_of_birth"], "decimals": {}},
    "agents": {"pk": "agent_id", "dates": [], "decimals": {}},
    "insurance_products": {"pk": "product_id", "dates": [], "decimals": {"base_monthly_premium": "decimal(12,2)", "max_cover_amount": "decimal(14,2)"}},
    "policies": {"pk": "policy_id", "dates": ["inception_date", "expiry_date", "cancellation_date"], "decimals": {"premium_amount": "decimal(12,2)", "sum_insured": "decimal(14,2)"}},
    "policy_coverages": {"pk": "policy_coverage_id", "dates": [], "decimals": {"coverage_limit": "decimal(14,2)", "deductible_amount": "decimal(12,2)"}},
    "policy_status_history": {"pk": "policy_status_history_id", "dates": ["changed_at"], "decimals": {}},
    "premium_transactions": {"pk": "premium_transaction_id", "dates": ["due_date", "transaction_date"], "decimals": {"amount": "decimal(12,2)"}},
    "claims": {"pk": "claim_id", "dates": ["loss_date", "reported_date", "closed_date"], "decimals": {"claimed_amount": "decimal(14,2)", "approved_amount": "decimal(14,2)"}},
    "claim_payments": {"pk": "claim_payment_id", "dates": ["payment_date"], "decimals": {"payment_amount": "decimal(14,2)"}},
}

ENUM_RULES = {
    "customers": {"customer_segment": ["MASS_MARKET", "AFFLUENT", "COMMERCIAL"]},
    "agents": {"active_flag": ["Y", "N"]},
    "insurance_products": {"active_flag": ["Y", "N"]},
    "policies": {"policy_status": ["PENDING", "ACTIVE", "LAPSED", "CANCELLED", "EXPIRED"]},
    "policy_coverages": {"active_flag": ["Y", "N"]},
    "premium_transactions": {"transaction_type": ["PREMIUM", "REVERSAL", "REFUND"], "transaction_status": ["PENDING", "PAID", "FAILED", "REVERSED"]},
    "claims": {"claim_status": ["OPEN", "UNDER_REVIEW", "APPROVED", "REJECTED", "SETTLED", "CLOSED"], "fraud_indicator": ["Y", "N"]},
    "claim_payments": {"payment_status": ["SCHEDULED", "PAID", "VOID"]},
}

GOLD_EXPORT_COLUMNS = {
    "customer_360": (
        "customer_id", "gender", "province", "customer_segment", "risk_score",
        "has_email", "age_years", "age_bin", "number_of_policies",
        "number_of_active_policies", "total_policy_premium_amount",
        "number_of_claims", "total_claimed_amount", "total_approved_amount",
        "number_of_payments", "total_paid_amount",
    ),
    "policy_performance": (
        "policy_id", "policy_number", "customer_id", "product_id", "agent_id",
        "policy_status", "payment_frequency", "premium_amount", "sum_insured",
        "underwriting_risk_band", "customer_segment", "province", "agent_code",
        "agent_name", "region", "channel", "product_name", "product_line",
        "total_coverage_limit", "claim_count", "total_claimed_amount",
        "total_approved_amount", "number_of_payments", "total_paid_amount",
        "failed_count", "paid_count", "pending_count", "reversed_count",
        "failed_total_amount", "paid_total_amount", "pending_total_amount",
        "reversed_total_amount",
    ),
    "claims_performance": (
        "claim_id", "claim_number", "policy_id", "claim_type", "loss_date",
        "reported_date", "claim_status", "claimed_amount", "approved_amount",
        "fraud_indicator", "adjuster_name", "closed_date", "created_at",
        "last_updated", "load_date", "customer_id", "product_id", "policy_status",
        "customer_segment", "province", "product_name", "product_line",
        "payment_count", "total_paid_amount",
    ),
    "premium_collection": (
        "due_month", "failed_count", "paid_count", "pending_count",
        "reversed_count", "failed_total_amount", "paid_total_amount",
        "pending_total_amount", "reversed_total_amount",
        "collection_rate_via_count", "collection_rate_via_amount",
    ),
    "policy_status_history": (
        "policy_status_history_id", "policy_id", "old_status", "new_status",
        "status_reason", "changed_at", "created_at", "last_updated", "load_date",
        "inception_date", "product_id", "customer_id", "product_line",
        "change_month", "days_since_inception", "is_lapse_or_cancel",
    ),
}


def _settings() -> dict[str, str]:
    load_dotenv(PROJECT_ROOT / ".env")
    required = ("MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_BUCKET")
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(f"Missing {', '.join(missing)} in environment or .env")
    return {key: os.environ[key] for key in required}


def ingest_table(table_name: str) -> int:
    """Atomically ingest one source table and advance only its watermark."""
    table_name = table_name.upper()
    if table_name not in RAW_TABLES:
        raise ValueError(f"Unsupported source table: {table_name}")
    settings = _settings()
    s3 = boto3.client("s3", endpoint_url=settings["MINIO_ENDPOINT"],
        aws_access_key_id=settings["MINIO_ACCESS_KEY"], aws_secret_access_key=settings["MINIO_SECRET_KEY"],
        region_name="us-east-1", config=Config(signature_version="s3v4"))
    with get_oracle_connection() as connection:
        watermark = pd.read_sql("SELECT last_watermark FROM etl_metadata WHERE table_name = :table_name", connection, params={"table_name": table_name})
        if watermark.empty:
            raise RuntimeError(f"ETL_METADATA has no row for {table_name}")
        last_watermark = watermark.iloc[0]["LAST_WATERMARK"]
        dataframe = pd.read_sql(f"SELECT * FROM {table_name} WHERE last_updated > :watermark", connection, params={"watermark": last_watermark})
        if dataframe.empty:
            print(f"{table_name}: no rows after watermark {last_watermark}")
            return 0
        buffer = BytesIO()
        dataframe.to_parquet(buffer, index=False)
        now = datetime.now(timezone.utc)
        key = f"raw/{table_name}/load_date={now:%Y-%m-%d}/{table_name}_{now:%Y%m%dT%H%M%S%fZ}.parquet"
        # Upload is deliberately before the Oracle transaction: a failed upload cannot lose data.
        s3.put_object(Bucket=settings["MINIO_BUCKET"], Key=key, Body=buffer.getvalue(), ContentType="application/octet-stream")
        new_watermark = dataframe["LAST_UPDATED"].max()
        with connection.cursor() as cursor:
            cursor.execute("UPDATE etl_metadata SET last_watermark = :watermark WHERE table_name = :table_name", {"watermark": new_watermark, "table_name": table_name})
        connection.commit()
    print(f"{table_name}: uploaded {len(dataframe)} rows to {key}; watermark={new_watermark}")
    return len(dataframe)


def _fail_if(dataframe: DataFrame, message: str) -> None:
    count = dataframe.count()
    if count:
        raise ValueError(f"{message}: {count} invalid row(s)")


def _clean_table(raw: DataFrame, config: dict) -> DataFrame:
    dataframe = raw.toDF(*[column.lower() for column in raw.columns]).withColumn("load_date", F.to_date("load_date"))
    latest = Window.partitionBy(config["pk"]).orderBy(F.col("last_updated").desc(), F.col("load_date").desc_nulls_last(), F.col("_source_file").desc())
    dataframe = dataframe.withColumn("_row_number", F.row_number().over(latest)).filter(F.col("_row_number") == 1).drop("_row_number")
    for column in config["dates"]:
        dataframe = dataframe.withColumn(column, F.col(column).cast("date"))
    for column, data_type in config["decimals"].items():
        dataframe = dataframe.withColumn(column, F.col(column).cast(data_type))
    return dataframe


def run_silver() -> None:
    settings = _settings(); bucket = settings["MINIO_BUCKET"]
    spark = get_spark_session("sanlam-raw-to-silver"); spark.sparkContext.setLogLevel("WARN")
    try:
        raw = {name: spark.read.parquet(f"s3a://{bucket}/raw/{name.upper()}/").withColumn("_source_file", F.input_file_name()) for name in TABLE_CONFIG}
        silver = {name: _clean_table(raw[name], config) for name, config in TABLE_CONFIG.items()}
        silver["customers"] = silver["customers"].withColumn("has_email", F.col("email").isNotNull())
        silver["policy_status_history"] = silver["policy_status_history"].withColumn("old_status", F.coalesce(F.col("old_status"), F.lit("NEW_POLICY")))
        silver["claims"] = silver["claims"].withColumn("approved_amount", F.when(F.col("claim_status") == "REJECTED", F.lit(None).cast("decimal(14,2)")).otherwise(F.col("approved_amount")))
        for name, config in TABLE_CONFIG.items():
            primary_key = config["pk"]; _fail_if(silver[name].filter(F.col(primary_key).isNull()), f"{name}.{primary_key} is null")
            _fail_if(silver[name].groupBy(primary_key).count().filter(F.col("count") > 1), f"{name}.{primary_key} is duplicated")
            for column, allowed in ENUM_RULES.get(name, {}).items(): _fail_if(silver[name].filter(F.col(column).isNotNull() & ~F.col(column).isin(allowed)), f"{name}.{column} is invalid")
        for child, foreign_key, parent, primary_key in (("policies","customer_id","customers","customer_id"),("policies","product_id","insurance_products","product_id"),("policies","agent_id","agents","agent_id"),("claims","policy_id","policies","policy_id"),("claim_payments","claim_id","claims","claim_id"),("premium_transactions","policy_id","policies","policy_id"),("policy_coverages","policy_id","policies","policy_id"),("policy_status_history","policy_id","policies","policy_id")):
            child_df = silver[child].filter(F.col(foreign_key).isNotNull()).alias("child")
            parent_df = silver[parent].select(primary_key).alias("parent")
            orphans = child_df.join(
                parent_df,
                F.col(f"child.{foreign_key}") == F.col(f"parent.{primary_key}"),
                "left_anti",
            )
            _fail_if(orphans, f"{child}.{foreign_key} has no parent")
        policies, claims, premiums = silver["policies"], silver["claims"], silver["premium_transactions"]
        _fail_if(policies.filter(F.date_add("inception_date", 365) != F.col("expiry_date")), "policy expiry is not inception plus 365 days")
        _fail_if(policies.filter((F.col("policy_status") == "CANCELLED") & F.col("cancellation_date").isNull()), "cancelled policy has no cancellation date")
        _fail_if(policies.filter((F.col("policy_status") != "CANCELLED") & F.col("cancellation_date").isNotNull()), "non-cancelled policy has a cancellation date")
        _fail_if(policies.filter((F.col("premium_amount") <= 0) | (F.col("sum_insured") <= 0)), "policy amount is not positive")
        _fail_if(claims.filter(F.col("loss_date") > F.col("reported_date")), "claim loss date is after reported date")
        _fail_if(claims.filter(F.col("approved_amount").isNotNull() & (F.col("approved_amount") > F.col("claimed_amount"))), "approved amount exceeds claimed amount")
        _fail_if(claims.filter(F.col("claim_status").isin("APPROVED", "SETTLED", "CLOSED") & F.col("approved_amount").isNull()), "decided claim has no approved amount")
        _fail_if(claims.filter(F.col("claim_status").isin("SETTLED", "CLOSED") & F.col("closed_date").isNull()), "closed claim has no closed date")
        _fail_if(premiums.filter((F.col("transaction_status") == "PAID") & F.col("transaction_date").isNull()), "paid premium has no transaction date")
        _fail_if(premiums.filter(F.col("amount") <= 0), "premium transaction amount is not positive")
        for name, dataframe in silver.items(): dataframe.drop("_source_file").write.mode("overwrite").option("compression", "snappy").parquet(f"s3a://{bucket}/silver/{name}/")
    finally:
        spark.stop()


def run_gold() -> None:
    settings = _settings(); bucket = settings["MINIO_BUCKET"]
    spark = get_spark_session("sanlam-silver-to-gold"); spark.sparkContext.setLogLevel("WARN")
    try:
        names = list(TABLE_CONFIG); silver = {name: spark.read.parquet(f"s3a://{bucket}/silver/{name}/") for name in names}
        customers, agents, products, policies = silver["customers"], silver["agents"], silver["insurance_products"], silver["policies"]
        coverages, premiums, claims, payments, history = silver["policy_coverages"], silver["premium_transactions"], silver["claims"], silver["claim_payments"], silver["policy_status_history"]
        paid_by_claim = payments.filter(F.col("payment_status") == "PAID").groupBy("claim_id").agg(F.count("*").alias("payment_count"), F.sum("payment_amount").alias("total_paid_amount"))
        claims_by_policy = claims.groupBy("policy_id").agg(F.count("*").alias("claim_count"), F.sum("claimed_amount").alias("total_claimed_amount"), F.sum(F.coalesce("approved_amount", F.lit(0))).alias("total_approved_amount"))
        payments_by_policy = claims.select("claim_id", "policy_id").join(paid_by_claim, "claim_id", "left").groupBy("policy_id").agg(F.sum(F.coalesce("payment_count", F.lit(0))).alias("number_of_payments"), F.sum(F.coalesce("total_paid_amount", F.lit(0))).alias("total_paid_amount"))
        coverage_by_policy = coverages.groupBy("policy_id").agg(F.sum("coverage_limit").alias("total_coverage_limit"))
        premium_by_policy = premiums.groupBy("policy_id").pivot("transaction_status", ["PAID", "FAILED", "PENDING", "REVERSED"]).agg(F.count("premium_transaction_id").alias("count"), F.sum("amount").alias("total_amount")).fillna(0)
        claims_performance = claims.join(policies.select("policy_id","customer_id","product_id","policy_status"),"policy_id","left").join(customers.select("customer_id","customer_segment","province"),"customer_id","left").join(products.select("product_id","product_name","product_line"),"product_id","left").join(paid_by_claim,"claim_id","left").fillna({"payment_count":0,"total_paid_amount":0})
        premium_collection = premiums.withColumn("due_month",F.trunc("due_date","month")).groupBy("due_month").pivot("transaction_status",["PAID","FAILED","PENDING","REVERSED"]).agg(F.count("premium_transaction_id").alias("count"),F.sum("amount").alias("total_amount")).fillna(0).withColumn("collection_rate_via_count",F.when((F.col("PAID_count")+F.col("FAILED_count")+F.col("PENDING_count")+F.col("REVERSED_count"))>0,F.col("PAID_count")/(F.col("PAID_count")+F.col("FAILED_count")+F.col("PENDING_count")+F.col("REVERSED_count")))).withColumn("collection_rate_via_amount",F.when((F.col("PAID_total_amount")+F.col("FAILED_total_amount")+F.col("PENDING_total_amount")+F.col("REVERSED_total_amount"))>0,F.col("PAID_total_amount")/(F.col("PAID_total_amount")+F.col("FAILED_total_amount")+F.col("PENDING_total_amount")+F.col("REVERSED_total_amount")))).orderBy("due_month")
        policy_performance = policies.select("policy_id","policy_number","customer_id","product_id","agent_id","policy_status","payment_frequency","premium_amount","sum_insured","underwriting_risk_band").join(customers.select("customer_id","customer_segment","province"),"customer_id","left").join(agents.select("agent_id","agent_code","agent_name","region","channel"),"agent_id","left").join(products.select("product_id","product_name","product_line"),"product_id","left").join(coverage_by_policy,"policy_id","left").join(claims_by_policy,"policy_id","left").join(payments_by_policy,"policy_id","left").join(premium_by_policy,"policy_id","left").fillna(0)
        customer_base = customers.select("customer_id","date_of_birth","gender","province","customer_segment","risk_score","has_email").withColumn("age_years",(F.datediff(F.current_date(),"date_of_birth")/F.lit(365.25)).cast("decimal(4,1)")).withColumn("age_bin",F.when(F.col("age_years")<25,"18-24").when(F.col("age_years")<35,"25-34").when(F.col("age_years")<45,"35-44").when(F.col("age_years")<55,"45-54").when(F.col("age_years")<65,"55-64").otherwise("65+")).drop("date_of_birth")
        customer_metrics = policies.select("policy_id","customer_id","policy_status","premium_amount").join(claims_by_policy,"policy_id","left").join(payments_by_policy,"policy_id","left").groupBy("customer_id").agg(F.count("policy_id").alias("number_of_policies"),F.sum(F.when(F.col("policy_status")=="ACTIVE",1).otherwise(0)).alias("number_of_active_policies"),F.sum("premium_amount").alias("total_policy_premium_amount"),F.sum(F.coalesce("claim_count",F.lit(0))).alias("number_of_claims"),F.sum(F.coalesce("total_claimed_amount",F.lit(0))).alias("total_claimed_amount"),F.sum(F.coalesce("total_approved_amount",F.lit(0))).alias("total_approved_amount"),F.sum(F.coalesce("number_of_payments",F.lit(0))).alias("number_of_payments"),F.sum(F.coalesce("total_paid_amount",F.lit(0))).alias("total_paid_amount"))
        status_gold = history.join(policies.select("policy_id","inception_date","product_id","customer_id"),"policy_id","left").join(products.select("product_id","product_line"),"product_id","left").withColumn("change_month",F.trunc("changed_at","month")).withColumn("days_since_inception",F.datediff("changed_at","inception_date")).withColumn("is_lapse_or_cancel",F.col("new_status").isin("LAPSED","CANCELLED"))
        gold = {"claims_performance":(claims_performance,"claim_id"),"premium_collection":(premium_collection,"due_month"),"policy_performance":(policy_performance,"policy_id"),"customer_360":(customer_base.join(customer_metrics,"customer_id","left").fillna(0),"customer_id"),"policy_status_history":(status_gold,"policy_status_history_id")}
        for name, (dataframe, key) in gold.items():
            _fail_if(dataframe.groupBy(key).count().filter(F.col("count") > 1), f"{name} violates grain {key}")
            dataframe.write.mode("overwrite").option("compression", "snappy").parquet(f"s3a://{bucket}/gold/{name}/")
    finally:
        spark.stop()


def export_tableau() -> None:
    settings = _settings(); bucket = settings["MINIO_BUCKET"]
    spark = get_spark_session("sanlam-export-gold-for-tableau"); spark.sparkContext.setLogLevel("WARN")
    try:
        destination = PROJECT_ROOT / "tableau_exports"; destination.mkdir(exist_ok=True)
        for name in ("customer_360", "policy_performance", "claims_performance", "premium_collection", "policy_status_history"):
            dataframe = spark.read.parquet(f"s3a://{bucket}/gold/{name}/")
            # Spark names multi-aggregation pivot columns with the pivot value
            # as a prefix. Normalize those names for the Tableau contract.
            for status in ("PAID", "FAILED", "PENDING", "REVERSED"):
                for suffix in ("count", "total_amount"):
                    source = f"{status}_{suffix}"
                    target = f"{status.lower()}_{suffix}"
                    if source in dataframe.columns and target not in dataframe.columns:
                        dataframe = dataframe.withColumnRenamed(source, target)
            expected = list(GOLD_EXPORT_COLUMNS[name])
            missing = [column for column in expected if column not in dataframe.columns]
            if missing:
                raise ValueError(f"{name} is missing Gold columns: {', '.join(missing)}")
            # Select by name, never by position, so CSV values cannot drift into
            # neighboring headers when Spark changes its physical column order.
            dataframe.select(*expected).toPandas().to_csv(
                destination / f"{name}.csv", index=False, encoding="utf-8"
            )
    finally:
        spark.stop()
