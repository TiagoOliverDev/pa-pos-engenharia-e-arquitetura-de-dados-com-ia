"""Configuracoes carregadas por variaveis de ambiente."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
import os

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency for local development
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()


def _as_int(value: str | None, default: int) -> int:
    if value is None or not value.strip():
        return default
    return int(value)


def _as_date(value: str | None, default: date) -> date:
    if value is None or not value.strip():
        return default
    return datetime.strptime(value, "%Y-%m-%d").date()


@dataclass(frozen=True, slots=True)
class Settings:
    """Conjunto de configuracoes usadas em todo o projeto."""

    app_env: str
    airflow_uid: str
    airflow_admin_user: str
    airflow_admin_password: str
    airflow_admin_email: str
    airflow_admin_first_name: str
    airflow_admin_last_name: str
    airflow_schedule: str
    airflow_retries: int
    airflow_retry_delay_seconds: int
    airflow_start_date: date
    default_election_year: int

    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str
    s3_bucket_name: str
    s3_endpoint_url: str | None

    project_root: Path
    src_root: Path
    dags_root: Path

    @classmethod
    def from_env(cls) -> "Settings":
        project_root = Path(__file__).resolve().parents[2]
        return cls(
            app_env=os.getenv("APP_ENV", "local"),
            airflow_uid=os.getenv("AIRFLOW_UID", "50000"),
            airflow_admin_user=os.getenv("AIRFLOW_ADMIN_USER", "airflow"),
            airflow_admin_password=os.getenv("AIRFLOW_ADMIN_PASSWORD", "airflow"),
            airflow_admin_email=os.getenv("AIRFLOW_ADMIN_EMAIL", "airflow@example.com"),
            airflow_admin_first_name=os.getenv("AIRFLOW_ADMIN_FIRST_NAME", "Airflow"),
            airflow_admin_last_name=os.getenv("AIRFLOW_ADMIN_LAST_NAME", "Admin"),
            airflow_schedule=os.getenv("AIRFLOW_DAG_SCHEDULE", "0 12 * * *"),
            airflow_retries=_as_int(os.getenv("AIRFLOW_RETRIES"), 1),
            airflow_retry_delay_seconds=_as_int(os.getenv("AIRFLOW_RETRY_DELAY_SECONDS"), 300),
            airflow_start_date=_as_date(os.getenv("AIRFLOW_START_DATE"), date(2024, 1, 1)),
            default_election_year=_as_int(os.getenv("DEFAULT_ELECTION_YEAR"), 2024),
            postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
            postgres_port=_as_int(os.getenv("POSTGRES_PORT"), 5432),
            postgres_db=os.getenv("POSTGRES_DB", "fefc_dw"),
            postgres_user=os.getenv("POSTGRES_USER", "fefc_user"),
            postgres_password=os.getenv("POSTGRES_PASSWORD", "fefc_password"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            aws_region=os.getenv("AWS_REGION", "sa-east-1"),
            s3_bucket_name=os.getenv("S3_BUCKET_NAME", "fefc-data-lake"),
            s3_endpoint_url=os.getenv("S3_ENDPOINT_URL") or None,
            project_root=project_root,
            src_root=project_root / "src",
            dags_root=project_root / "dags",
        )

    @property
    def postgres_sqlalchemy_url(self) -> str:
        return (
            "postgresql+psycopg2://"
            f"{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def airflow_sqlalchemy_url(self) -> str:
        return (
            "postgresql+psycopg2://"
            f"{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/airflow"
        )

    @property
    def use_localstack(self) -> bool:
        return self.s3_endpoint_url is not None


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings so the rest of the project can reuse them."""

    return Settings.from_env()
