"""DAG inicial do projeto FEFC."""

from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import dag, task

LOGGER = logging.getLogger(__name__)
DEFAULT_START_DATE = datetime(2024, 1, 1)
DEFAULT_SCHEDULE = "0 12 * * *"
DEFAULT_RETRIES = 1
DEFAULT_RETRY_DELAY_SECONDS = 300


@dag(
    dag_id="fundo_eleitoral_pipeline",
    schedule=DEFAULT_SCHEDULE,
    start_date=DEFAULT_START_DATE,
    catchup=False,
    default_args={
        "retries": DEFAULT_RETRIES,
        "retry_delay": timedelta(seconds=DEFAULT_RETRY_DELAY_SECONDS),
    },
    tags=["fefc", "bronze", "silver", "gold"],
    description="Pipeline inicial do MVP FEFC",
)
def fundo_eleitoral_pipeline() -> None:
    """Orquestra as camadas do projeto sem aplicar a regra final de negocio."""

    @task
    def ingest() -> dict[str, Any]:
        from src.bronze.storage import S3Storage
        from src.ingestion.tse_client import TSEClient

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
        from src.bronze.storage import S3Storage

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
        from src.silver.transformations import normalize_records

        silver_records = normalize_records(payload["manifest"])
        LOGGER.info("Camada Silver preparada com %s registros.", len(silver_records))
        return {**payload, "silver_records": silver_records}

    @task
    def validate(payload: dict[str, Any]) -> dict[str, Any]:
        from src.quality.validations import validate_records

        report = validate_records(payload["silver_records"], allow_empty=True)
        return {**payload, "validation": asdict(report)}

    @task
    def load_gold(payload: dict[str, Any]) -> dict[str, Any]:
        from src.gold.loader import PostgresWarehouse

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
    # silver_payload = transform_silver(bronze_payload)
    # validated_payload = validate(silver_payload)
    # load_gold(validated_payload)


fundo_eleitoral_pipeline()
