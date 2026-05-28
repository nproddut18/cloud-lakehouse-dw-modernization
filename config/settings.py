"""
Global configuration — reads from environment variables with safe defaults.
All secrets must be set as environment variables; never hard-code credentials.
"""

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class S3Config:
    bucket: str = os.getenv("S3_BUCKET", "naren-lakehouse-prod")
    bronze_prefix: str = "bronze"
    silver_prefix: str = "silver"
    gold_prefix: str = "gold"
    region: str = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    def bronze_path(self, table: str) -> str:
        return f"s3://{self.bucket}/{self.bronze_prefix}/{table}/"

    def silver_path(self, table: str) -> str:
        return f"s3://{self.bucket}/{self.silver_prefix}/{table}/"

    def gold_path(self, table: str) -> str:
        return f"s3://{self.bucket}/{self.gold_prefix}/{table}/"


@dataclass(frozen=True)
class RedshiftConfig:
    host: str = os.getenv("REDSHIFT_HOST", "")
    port: int = int(os.getenv("REDSHIFT_PORT", "5439"))
    database: str = os.getenv("REDSHIFT_DB", "lakehouse")
    user: str = os.getenv("REDSHIFT_USER", "admin")
    password: str = os.getenv("REDSHIFT_PASSWORD", "")
    schema: str = os.getenv("REDSHIFT_SCHEMA", "warehouse")
    iam_role: str = os.getenv("REDSHIFT_IAM_ROLE", "")

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:redshift://{self.host}:{self.port}/{self.database}"
            f"?user={self.user}&password={self.password}"
        )


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
    user: str = os.getenv("SNOWFLAKE_USER", "naren_de")
    password: str = os.getenv("SNOWFLAKE_PASSWORD", "")
    warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    database: str = os.getenv("SNOWFLAKE_DATABASE", "LAKEHOUSE_DB")
    schema: str = os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS")
    role: str = os.getenv("SNOWFLAKE_ROLE", "DATA_ENGINEER")


@dataclass(frozen=True)
class SparkConfig:
    app_name: str = "LakehousePipeline"
    master: str = os.getenv("SPARK_MASTER", "local[*]")
    executor_memory: str = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
    driver_memory: str = os.getenv("SPARK_DRIVER_MEMORY", "2g")
    shuffle_partitions: int = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "200"))
    delta_log_retention_days: int = 7


@dataclass(frozen=True)
class PipelineConfig:
    env: str = os.getenv("PIPELINE_ENV", "dev")  # dev | staging | prod
    batch_date: str = os.getenv("BATCH_DATE", "")  # YYYY-MM-DD
    max_retries: int = 3
    retry_delay_seconds: int = 60
    alert_email: str = os.getenv("ALERT_EMAIL", "nproddut18@gmail.com")


# Singleton instances — import these throughout the project
S3 = S3Config()
REDSHIFT = RedshiftConfig()
SNOWFLAKE = SnowflakeConfig()
SPARK = SparkConfig()
PIPELINE = PipelineConfig()
