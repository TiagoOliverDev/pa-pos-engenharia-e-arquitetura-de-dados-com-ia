"""DAG inicial do projeto FEFC."""

from __future__ import annotations

from dataclasses import asdict
from datetime import timedelta
from typing import Any

from airflow.decorators import dag, task

from src.bronze.storage import S3Storage
from src.config import get_settings
from src.gold.loader import PostgresWarehouse
from src.ingestion.tse_client import TSEClient
from src.quality.validations import validate_records
from src.silver.transformations import normalize_records
from src.utils.logging import get_logger

LOGGER = get_logger(__name__)
SETTINGS = get_settings()


@dag(
    dag_id="fundo_eleitoral_pipeline",
    schedule=SETTINGS.airflow_schedule,
    start_date=SETTINGS.airflow_start_date,
    catchup=False,
    default_args={
        "retries": SETTINGS.airflow_retries,
        "retry_delay": timedelta(seconds=SETTINGS.airflow_retry_delay_seconds),
    },
    tags=["fefc", "bronze", "silver", "gold"],
    description="Pipeline inicial do MVP FEFC",
)
def fundo_eleitoral_pipeline() -> None:
    """Orquestra as camadas do projeto sem aplicar a regra final de negocio."""

    @task
    def ingest() -> dict[str, Any]:
        client = TSEClient()
        storage = S3Storage()
        manifest = client.ingest_to_bronze(storage)
        LOGGER.info(
            "Escopo FEFC definido com os anos %s e fonte oficial do TSE.",
            client.list_election_years(),
        )
        return {
            "scope": {
                "source_name": client.scope.source_name,
                "source_url": client.scope.source_url,
                "election_years": list(client.list_election_years()),
            },
            "manifest": manifest,
        }

    @task
    def store_bronze(payload: dict[str, Any]) -> dict[str, Any]:
        storage = S3Storage()
        bronze_prefixes = sorted(
            {
                storage.paths.bronze_prefix(item["election_year"])
                for item in payload["manifest"]
            }
        )
        for prefix in bronze_prefixes:
            LOGGER.info("Camada Bronze preparada em %s.", prefix)
        return {**payload, "bronze_prefixes": bronze_prefixes}

    @task
    def transform_silver(payload: dict[str, Any]) -> dict[str, Any]:
        silver_records = normalize_records(payload["manifest"])
        LOGGER.info("Camada Silver preparada com %s registros.", len(silver_records))
        return {**payload, "silver_records": silver_records}

    @task
    def validate(payload: dict[str, Any]) -> dict[str, Any]:
        report = validate_records(payload["silver_records"], allow_empty=True)
        return {**payload, "validation": asdict(report)}

    @task
    def load_gold(payload: dict[str, Any]) -> dict[str, Any]:
        warehouse = PostgresWarehouse()
        LOGGER.info("Conexao Gold preparada para %s.", warehouse.settings.postgres_db)
        return {
            **payload,
            "gold_target": {
                "host": warehouse.settings.postgres_host,
                "port": warehouse.settings.postgres_port,
                "database": warehouse.settings.postgres_db,
            },
            "gold_ready": True,
        }

    ingest_payload = ingest()
    bronze_payload = store_bronze(ingest_payload)
    silver_payload = transform_silver(bronze_payload)
    validated_payload = validate(silver_payload)
    load_gold(validated_payload)


fundo_eleitoral_pipeline()
