"""Abstracao de armazenamento em S3."""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Any

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover - fallback for minimal local environments
    boto3 = None

    class ClientError(Exception):
        """Fallback client error when boto3 is unavailable."""

from src.config import Settings, get_settings
from src.utils.logging import get_logger

LOGGER = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class S3PathBuilder:
    """Construtor de paths padronizados para as camadas do lake."""

    bucket_name: str
    dataset_name: str = "fundo_eleitoral"

    def build_prefix(self, layer: str, election_year: int) -> str:
        return self.build_partitioned_prefix(layer, {"ano_eleicao": election_year})

    def build_partitioned_prefix(self, layer: str, partitions: dict[str, Any]) -> str:
        partition_bits = "/".join(f"{name}={value}" for name, value in partitions.items())
        return f"{layer}/{self.dataset_name}/{partition_bits}/"

    def bronze_prefix(self, election_year: int) -> str:
        return self.build_prefix("bronze", election_year)

    def silver_prefix(self, election_year: int) -> str:
        return self.build_prefix("silver", election_year)

    def silver_treated_prefix(self, election_year: int) -> str:
        return f"{self.silver_prefix(election_year)}tratado/"

    def raw_bronze_prefix(self, election_year: int) -> str:
        return f"{self.build_partitioned_prefix('bronze', {'ano_eleicao': election_year})}raw/"

    def build_key(self, layer: str, election_year: int, filename: str) -> str:
        return f"{self.build_prefix(layer, election_year)}{filename}"

    def build_treated_silver_key(self, election_year: int, filename: str) -> str:
        return f"{self.silver_treated_prefix(election_year)}{filename}"

    def build_raw_key(self, election_year: int, filename: str) -> str:
        return f"{self.raw_bronze_prefix(election_year)}{filename}"


class S3Storage:
    """Pequena camada de acesso ao S3 para evitar boto3 espalhado no projeto."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._client = None
        if boto3 is not None:
            client_kwargs: dict[str, Any] = {"region_name": self._settings.aws_region}
            if self._settings.s3_endpoint_url:
                client_kwargs["endpoint_url"] = self._settings.s3_endpoint_url
            self._client = boto3.client(
                "s3",
                aws_access_key_id=self._settings.aws_access_key_id,
                aws_secret_access_key=self._settings.aws_secret_access_key,
                **client_kwargs,
            )
        self._paths = S3PathBuilder(bucket_name=self._settings.s3_bucket_name)

    @property
    def bucket_name(self) -> str:
        return self._settings.s3_bucket_name

    @property
    def paths(self) -> S3PathBuilder:
        return self._paths

    def _require_client(self) -> Any:
        if self._client is None:
            raise RuntimeError(
                "boto3 nao esta instalado neste ambiente; a camada S3 exige a dependencia."
            )
        return self._client

    def ensure_bucket(self) -> None:
        """Create the bucket if needed.

        The method is safe for LocalStack development and also works for real AWS.
        """

        try:
            self._require_client().head_bucket(Bucket=self.bucket_name)
            return
        except ClientError:
            LOGGER.info("Bucket %s nao existe ainda; sera criado.", self.bucket_name)

        create_kwargs: dict[str, Any] = {"Bucket": self.bucket_name}
        if self._settings.aws_region != "us-east-1":
            create_kwargs["CreateBucketConfiguration"] = {
                "LocationConstraint": self._settings.aws_region
            }
        self._require_client().create_bucket(**create_kwargs)

    def object_exists(self, key: str) -> bool:
        try:
            self._require_client().head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError:
            return False

    def upload_bytes(self, key: str, payload: bytes, content_type: str = "application/octet-stream") -> None:
        self._require_client().put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=BytesIO(payload).getvalue(),
            ContentType=content_type,
        )

    def upload_text(self, key: str, content: str, content_type: str = "text/plain; charset=utf-8") -> None:
        self.upload_bytes(key, content.encode("utf-8"), content_type=content_type)

    def download_bytes(self, key: str) -> bytes:
        response = self._require_client().get_object(Bucket=self.bucket_name, Key=key)
        return response["Body"].read()

    def list_objects(self, prefix: str = "") -> list[str]:
        response = self._require_client().list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        return [item["Key"] for item in response.get("Contents", [])]
