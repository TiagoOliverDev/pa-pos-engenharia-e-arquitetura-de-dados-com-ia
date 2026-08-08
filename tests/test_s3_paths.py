from src.bronze.storage import S3PathBuilder


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
