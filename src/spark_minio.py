"""Create a Spark session configured for this project's local MinIO lake."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession


ENV_FILE = Path(__file__).resolve().parent.parent / ".env"
REQUIRED_SETTINGS = ("MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY")
LOCAL_SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


def _existing_s3a_jars() -> tuple[str, str] | None:
    candidates = (
        (
            Path("/opt/spark-extra-jars/hadoop-aws-3.4.1.jar"),
            Path("/opt/spark-extra-jars/aws-sdk-bundle-2.24.6.jar"),
        ),
        (
            Path("/opt/airflow/hadoop-aws-3.4.1.jar"),
            Path("/opt/airflow/aws-sdk-bundle-2.24.6.jar"),
        ),
    )
    for hadoop_aws, aws_bundle in candidates:
        if hadoop_aws.exists() and aws_bundle.exists():
            jars = f"{hadoop_aws},{aws_bundle}"
            return jars, jars.replace(",", ":")
    return None


def get_spark_session(app_name: str) -> SparkSession:
    """Return Spark configured to read and write s3a:// paths in MinIO."""
    load_dotenv(ENV_FILE)
    missing = [setting for setting in REQUIRED_SETTINGS if not os.getenv(setting)]
    if missing:
        raise RuntimeError(f"Missing {', '.join(missing)} in {ENV_FILE.name}.")

    endpoint = os.environ["MINIO_ENDPOINT"].rstrip("/")
    if not endpoint.startswith(("http://", "https://")):
        endpoint = f"http://{endpoint}"

    builder = (
        SparkSession.builder
        .appName(app_name)
        # Some bundled Hadoop/S3A builds reject time values with suffixes such as "60s".
        # Override the S3A thread keepalive explicitly so S3A init stays numeric-only.
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.connection.acquisition.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.idle.time", "60000")
        .config("spark.hadoop.fs.s3a.connection.ttl", "300000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "30000")
        .config("spark.hadoop.fs.s3a.connection.maximum", "200")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(endpoint.startswith("https://")).lower(),
        )
    )

    jar_config = _existing_s3a_jars()
    if jar_config:
        spark_jars, spark_extra_classpath = jar_config
        builder = (
            builder
            .config("spark.jars", spark_jars)
            .config("spark.driver.extraClassPath", spark_extra_classpath)
            .config("spark.executor.extraClassPath", spark_extra_classpath)
        )
    else:
        # Local notebooks do not have the Airflow image's baked-in jars.
        # Restore the pre-Airflow behavior for that environment only.
        builder = builder.config("spark.jars.packages", LOCAL_SPARK_PACKAGES)

    return builder.getOrCreate()
