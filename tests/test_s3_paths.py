from src.bronze.storage import ClientError
from src.bronze.storage import S3PathBuilder
from src.bronze.storage import S3Storage
from src.config.settings import Settings


def test_bronze_prefix_building() -> None:
    builder = S3PathBuilder(bucket_name="bucket")

    assert builder.bronze_prefix(2024) == "bronze/fundo_eleitoral/ano_eleicao=2024/"
    assert builder.raw_bronze_prefix(2024) == "bronze/fundo_eleitoral/ano_eleicao=2024/raw/"
    assert builder.build_raw_key(2024, "fefc_fp_2024.zip") == (
        "bronze/fundo_eleitoral/ano_eleicao=2024/raw/fefc_fp_2024.zip"
    )
    assert builder.build_partitioned_prefix("bronze", {"ano_eleicao": 2024}) == (
        "bronze/fundo_eleitoral/ano_eleicao=2024/"
    )


def test_ensure_bucket_sends_location_constraint_when_region_is_specific(monkeypatch) -> None:
    calls = {}

    class _FakeClient:
        def head_bucket(self, Bucket):
            raise ClientError("not found")

        def create_bucket(self, **kwargs):
            calls["kwargs"] = kwargs

    def fake_get_settings():
        return Settings(
            app_env="local",
            airflow_uid="50000",
            airflow_admin_user="airflow",
            airflow_admin_password="airflow",
            airflow_admin_email="airflow@example.com",
            airflow_admin_first_name="Airflow",
            airflow_admin_last_name="Admin",
            airflow_schedule="0 12 * * *",
            airflow_retries=1,
            airflow_retry_delay_seconds=300,
            airflow_start_date=__import__("datetime").date(2024, 1, 1),
            default_election_year=2024,
            postgres_host="postgres",
            postgres_port=5432,
            postgres_db="fefc_dw",
            postgres_user="fefc_user",
            postgres_password="fefc_password",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            aws_region="sa-east-1",
            s3_bucket_name="bucket",
            s3_endpoint_url="http://localstack:4566",
            project_root=__import__("pathlib").Path("."),
            src_root=__import__("pathlib").Path("./src"),
            dags_root=__import__("pathlib").Path("./dags"),
        )

    monkeypatch.setattr("src.bronze.storage.boto3", None)
    monkeypatch.setattr("src.bronze.storage.get_settings", fake_get_settings)

    storage = S3Storage()
    storage._client = _FakeClient()

    storage.ensure_bucket()

    assert calls["kwargs"]["Bucket"] == "bucket"
    assert calls["kwargs"]["CreateBucketConfiguration"]["LocationConstraint"] == "sa-east-1"
