from __future__ import annotations

from lakehouse.common.config import load_config
from lakehouse.common.storage import S3Path, to_spark_path


def test_load_config_reads_env_file_and_expands_s3_paths(tmp_path, monkeypatch):
    for name in [
        "LAKEHOUSE_ENV",
        "LAKEHOUSE_CONFIG_DIR",
        "LAKEHOUSE_ENV_FILE",
        "S3_BUCKET",
        "SPARK_ENABLE_DELTA",
    ]:
        monkeypatch.delenv(name, raising=False)

    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    (config_dir / "prod.yaml").write_text(
        "\n".join(
            [
                "environment: prod",
                "project_root: .",
                "raw_root: s3://${S3_BUCKET}/raw",
                "lakehouse_root: s3://${S3_BUCKET}/lakehouse",
                "checkpoint_root: s3://${S3_BUCKET}/metadata/checkpoints",
                "report_root: s3://${S3_BUCKET}/reports",
                "default_format: parquet",
                "write_mode: append",
                "spark:",
                "  enable_delta: ${SPARK_ENABLE_DELTA:-true}",
            ]
        ),
        encoding="utf-8",
    )
    env_file = tmp_path / ".env.prod"
    env_file.write_text(
        "\n".join(
            [
                "LAKEHOUSE_ENV=prod",
                "S3_BUCKET=riot-test-bucket",
            ]
        ),
        encoding="utf-8",
    )

    config = load_config(config_dir=config_dir, env_file=env_file)

    assert isinstance(config.raw_root, S3Path)
    assert config.raw_root.as_posix() == "s3://riot-test-bucket/raw"
    assert config.layer_path("silver", "matches").as_posix() == (
        "s3://riot-test-bucket/lakehouse/silver/matches"
    )
    assert config.values["spark"]["enable_delta"] is True


def test_s3_path_converts_to_spark_s3a_uri():
    assert to_spark_path(S3Path("s3://riot-test-bucket/lakehouse")) == (
        "s3a://riot-test-bucket/lakehouse"
    )
