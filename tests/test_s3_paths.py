from src.bronze.storage import S3PathBuilder


def test_bronze_prefix_building() -> None:
    builder = S3PathBuilder(bucket_name="bucket")

    assert builder.bronze_prefix(2024) == "bronze/fundo_eleitoral/ano_eleicao=2024/"

