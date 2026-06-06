from __future__ import annotations

import argparse
import os
from typing import Any

from lakehouse.common.config import LakehouseConfig
from lakehouse.common.storage import S3Path, is_s3_path, to_spark_path
from lakehouse.jobs._cli import add_config_args, load_config_from_args


def _optional_env(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    normalized = value.strip().strip('"').strip("'")
    if not normalized:
        os.environ.pop(name, None)
        return None
    return normalized


def _status(state: str, label: str, message: str) -> None:
    print(f"[{state}] {label}: {message}")


def _fail(failures: list[str], label: str, message: str) -> None:
    failures.append(label)
    _status("FAIL", label, message)


def _load_config(args: argparse.Namespace, failures: list[str]) -> LakehouseConfig | None:
    try:
        config = load_config_from_args(args)
    except Exception as exc:
        _fail(failures, "config", str(exc))
        return None

    _status(
        "OK",
        "config",
        (
            f"environment={config.environment}; raw_root={config.raw_root}; "
            f"lakehouse_root={config.lakehouse_root}"
        ),
    )
    return config


def _import_boto3(failures: list[str]) -> Any | None:
    try:
        import boto3
    except ImportError as exc:
        _fail(failures, "boto3", f"not importable: {exc}")
        return None

    _status("OK", "boto3", "imported")
    return boto3


def _make_s3_client(boto3: Any, failures: list[str]) -> Any | None:
    profile = _optional_env("AWS_PROFILE")
    region = _optional_env("AWS_REGION") or _optional_env("AWS_DEFAULT_REGION")
    endpoint_url = _optional_env("AWS_S3_ENDPOINT_URL") or _optional_env("AWS_ENDPOINT_URL")

    try:
        session_kwargs = {"region_name": region}
        if profile:
            session_kwargs["profile_name"] = profile
        session = boto3.Session(**session_kwargs)
        credentials = session.get_credentials()
    except Exception as exc:
        _fail(failures, "aws credentials", f"could not resolve credentials: {exc}")
        return None

    if credentials is None:
        _fail(failures, "aws credentials", "no credentials found")
        return None

    method = getattr(credentials, "method", "unknown")
    _status("OK", "aws credentials", f"available via {method}")

    try:
        from botocore.config import Config

        client_config = Config(
            connect_timeout=10,
            read_timeout=10,
            retries={"max_attempts": 2, "mode": "standard"},
        )
    except Exception:
        client_config = None

    try:
        return session.client("s3", endpoint_url=endpoint_url, config=client_config)
    except Exception as exc:
        _fail(failures, "s3 client", f"could not create client: {exc}")
        return None


def _check_bucket_env(failures: list[str]) -> None:
    bucket = _optional_env("S3_BUCKET")
    if bucket:
        _status("OK", "S3_BUCKET", bucket)
    else:
        _fail(failures, "S3_BUCKET", "not set")


def _list_prefix(client: Any, path: Any, label: str, failures: list[str]) -> None:
    if not isinstance(path, S3Path):
        _status("SKIP", label, f"{path} is not an S3 path")
        return

    prefix = f"{path.key.rstrip('/')}/" if path.key else ""
    try:
        response = client.list_objects_v2(Bucket=path.bucket, Prefix=prefix, MaxKeys=1)
    except Exception as exc:
        _fail(failures, label, f"cannot list {path}: {exc}")
        return

    key_count = response.get("KeyCount", len(response.get("Contents", [])))
    detail = "empty but listable" if key_count == 0 else f"found {key_count} sample object(s)"
    _status("OK", label, f"{path} is {detail}")


def _find_bronze_parquet(client: Any, bronze_path: Any, max_scan: int) -> S3Path | None:
    if not isinstance(bronze_path, S3Path):
        return None

    prefix = f"{bronze_path.key.rstrip('/')}/" if bronze_path.key else ""
    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(
        Bucket=bronze_path.bucket,
        Prefix=prefix,
        PaginationConfig={"MaxItems": max_scan, "PageSize": min(max_scan, 1000)},
    )
    best_key: str | None = None
    best_size: int | None = None
    for page in pages:
        for item in page.get("Contents", []):
            key = item.get("Key")
            size = int(item.get("Size", 0))
            if not key or not key.endswith(".parquet") or size <= 0:
                continue
            if best_size is None or size < best_size:
                best_key = key
                best_size = size

    if best_key is None:
        return None
    return S3Path(f"s3://{bronze_path.bucket}/{best_key}")


def _check_spark_parquet_read(
    config: LakehouseConfig,
    parquet_path: S3Path,
    failures: list[str],
) -> None:
    spark = None
    try:
        from lakehouse.common.spark import get_spark

        spark = get_spark(app_name="riot-lakehouse-preflight", config=config)
        rows = spark.read.parquet(to_spark_path(parquet_path)).limit(1).count()
    except Exception as exc:
        _fail(failures, "spark bronze parquet read", f"{parquet_path}: {exc}")
        return
    finally:
        if spark is not None:
            spark.stop()

    _status("OK", "spark bronze parquet read", f"{parquet_path}; sampled {rows} row(s)")


def _config_uses_s3(config: LakehouseConfig) -> bool:
    return any(
        is_s3_path(path)
        for path in [
            config.raw_root,
            config.lakehouse_root,
            config.checkpoint_root,
            config.report_root,
        ]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Check lakehouse runtime environment")
    add_config_args(parser)
    parser.add_argument(
        "--skip-spark-read",
        action="store_true",
        help="Skip the optional Bronze Parquet sample read.",
    )
    parser.add_argument(
        "--max-parquet-scan",
        type=int,
        default=1000,
        help="Maximum S3 objects to scan while looking for a Bronze Parquet sample.",
    )
    args = parser.parse_args()

    failures: list[str] = []
    config = _load_config(args, failures)
    selected_env = args.env or _optional_env("LAKEHOUSE_ENV") or "dev"
    _status("INFO", "LAKEHOUSE_ENV", f"{_optional_env('LAKEHOUSE_ENV') or '<unset>'}")
    _status("INFO", "selected env", selected_env)

    if config is None:
        if selected_env != "dev":
            _check_bucket_env(failures)
        raise SystemExit(1)
    if not _config_uses_s3(config):
        _status("SKIP", "s3 checks", f"{config.environment} does not use S3 paths")
        raise SystemExit(1 if failures else 0)

    _check_bucket_env(failures)
    boto3 = _import_boto3(failures)
    if boto3 is None:
        raise SystemExit(1)

    client = _make_s3_client(boto3, failures)
    if client is None:
        raise SystemExit(1)

    bronze_path = config.layer_path("bronze")
    _list_prefix(client, config.raw_root, "s3 raw prefix", failures)
    _list_prefix(client, config.lakehouse_root, "s3 lakehouse prefix", failures)
    _list_prefix(client, bronze_path, "s3 bronze prefix", failures)

    if not args.skip_spark_read:
        try:
            sample = _find_bronze_parquet(client, bronze_path, args.max_parquet_scan)
        except Exception as exc:
            _fail(failures, "bronze parquet discovery", str(exc))
        else:
            if sample is None:
                _status("SKIP", "spark bronze parquet read", "no Bronze Parquet sample found")
            else:
                _check_spark_parquet_read(config, sample, failures)

    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
