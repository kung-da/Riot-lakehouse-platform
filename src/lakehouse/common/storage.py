from __future__ import annotations

import fnmatch
import io
import logging
import os
import posixpath
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator
from urllib.parse import urlsplit


S3_SCHEMES = ("s3://", "s3a://")
LOGGER = logging.getLogger(__name__)


def is_s3_uri(value: str) -> bool:
    return value.startswith(S3_SCHEMES)


def is_s3_path(path: Any) -> bool:
    return isinstance(path, S3Path) or is_s3_uri(str(path))


def path_text(path: Any) -> str:
    if hasattr(path, "as_posix"):
        return str(path.as_posix())
    return str(path)


def to_spark_path(path: Any) -> str:
    text = path_text(path)
    if text.startswith("s3:/") and not text.startswith("s3://"):
        text = "s3://" + text.removeprefix("s3:/").lstrip("/")
    if text.startswith("s3://"):
        return "s3a://" + text.removeprefix("s3://")
    return text


def has_files(path: Any, pattern: str) -> bool:
    if isinstance(path, S3Path):
        return any(path.rglob(pattern))
    return path.exists() and any(path.rglob(pattern))


def _s3_client() -> Any:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError(
            "Install the aws extra to use S3 paths: pip install -e '.[aws]'"
        ) from exc

    def optional_env(name: str) -> str | None:
        value = os.getenv(name)
        if value is None:
            return None
        normalized = value.strip().strip('"').strip("'")
        if not normalized:
            os.environ.pop(name, None)
            return None
        return normalized

    profile = optional_env("AWS_PROFILE")
    region_name = optional_env("AWS_REGION") or optional_env("AWS_DEFAULT_REGION")
    endpoint_url = optional_env("AWS_S3_ENDPOINT_URL") or optional_env("AWS_ENDPOINT_URL")
    session = boto3.Session(profile_name=profile, region_name=region_name)
    return session.client("s3", endpoint_url=endpoint_url)


def delete_s3_prefix(path: Any) -> int:
    target = path if isinstance(path, S3Path) else S3Path(str(path))
    prefix = target.key.rstrip("/")
    if not prefix:
        raise ValueError("Refusing to delete an entire S3 bucket")
    prefix = f"{prefix}/"
    client = _s3_client()
    deleted = 0
    batch: list[dict[str, str]] = []
    for key in target._iter_keys(prefix):
        batch.append({"Key": key})
        if len(batch) == 1000:
            client.delete_objects(Bucket=target.bucket, Delete={"Objects": batch})
            deleted += len(batch)
            batch = []
    if batch:
        client.delete_objects(Bucket=target.bucket, Delete={"Objects": batch})
        deleted += len(batch)
    return deleted


def _uploadable_files(local_dir: Path) -> list[Path]:
    files: list[Path] = []
    for file_path in local_dir.rglob("*"):
        if not file_path.is_file():
            continue
        relative_key = file_path.relative_to(local_dir).as_posix()
        if relative_key.startswith("_temporary/") or "/_temporary/" in relative_key:
            continue
        if file_path.name.startswith(".") and file_path.name.endswith(".crc"):
            continue
        files.append(file_path)
    return files


def upload_directory_to_s3(local_dir: Path | str, destination: Any, max_workers: int = 8) -> int:
    local_path = Path(local_dir)
    target = destination if isinstance(destination, S3Path) else S3Path(str(destination))
    client = _s3_client()
    files = _uploadable_files(local_path)
    if not files:
        return 0

    LOGGER.info("Uploading %s files from %s to %s", len(files), local_path, target)

    def upload_one(file_path: Path) -> None:
        relative_key = file_path.relative_to(local_path).as_posix()
        key = posixpath.join(target.key.rstrip("/"), relative_key) if target.key else relative_key
        client.upload_file(str(file_path), target.bucket, key)

    uploaded = 0
    workers = max(1, max_workers)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(upload_one, file_path) for file_path in files]
        for future in as_completed(futures):
            future.result()
            uploaded += 1
            if uploaded % 250 == 0 or uploaded == len(files):
                LOGGER.info("Uploaded %s/%s files to %s", uploaded, len(files), target)
    return uploaded


def write_parquet_dataset(
    dataframe: Any,
    output_path: Any,
    mode: str,
    partition_columns: list[str],
    output_partitions: int,
) -> None:
    if output_partitions < 1:
        raise ValueError("output_partitions must be greater than zero")

    dataframe = dataframe.coalesce(output_partitions)
    writer = dataframe.write.option("compression", "snappy")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    if not is_s3_path(output_path):
        writer.mode(mode).parquet(to_spark_path(output_path))
        return

    normalized_mode = mode.lower()
    if normalized_mode not in {"append", "overwrite"}:
        raise ValueError("S3 Parquet writes support only append and overwrite modes")

    with tempfile.TemporaryDirectory(prefix="lakehouse-parquet-") as tmp_dir:
        local_output = Path(tmp_dir) / "output"
        writer.mode("overwrite").parquet(local_output.as_posix())
        if normalized_mode == "overwrite":
            delete_s3_prefix(output_path)
        upload_directory_to_s3(local_output, output_path)


class _S3TextWriter(io.StringIO):
    def __init__(self, path: "S3Path", encoding: str) -> None:
        super().__init__()
        self._path = path
        self._encoding = encoding

    def close(self) -> None:
        if not self.closed:
            self._path.write_text(self.getvalue(), encoding=self._encoding)
        super().close()


@dataclass(frozen=True)
class S3Path:
    uri: str

    def __post_init__(self) -> None:
        parsed = urlsplit(self.uri)
        if parsed.scheme not in {"s3", "s3a"}:
            raise ValueError(f"Expected s3:// or s3a:// URI, got {self.uri!r}")
        if not parsed.netloc:
            raise ValueError(f"S3 URI is missing a bucket: {self.uri!r}")
        key = parsed.path.lstrip("/")
        normalized = f"s3://{parsed.netloc}" + (f"/{key}" if key else "")
        object.__setattr__(self, "uri", normalized)

    @property
    def bucket(self) -> str:
        return urlsplit(self.uri).netloc

    @property
    def key(self) -> str:
        return urlsplit(self.uri).path.lstrip("/")

    @property
    def name(self) -> str:
        return posixpath.basename(self.key.rstrip("/"))

    @property
    def parent(self) -> "S3Path":
        if not self.key:
            return self
        parent_key = posixpath.dirname(self.key.rstrip("/"))
        return self._with_key(parent_key)

    def _with_key(self, key: str) -> "S3Path":
        clean_key = key.strip("/")
        return S3Path(f"s3://{self.bucket}" + (f"/{clean_key}" if clean_key else ""))

    def __truediv__(self, value: str) -> "S3Path":
        value_text = str(value)
        if is_s3_uri(value_text):
            return S3Path(value_text)
        clean_value = value_text.replace("\\", "/").strip("/")
        if not clean_value:
            return self
        key = f"{self.key.rstrip('/')}/{clean_value}" if self.key else clean_value
        return self._with_key(key)

    def __fspath__(self) -> str:
        return self.uri

    def __str__(self) -> str:
        return self.uri

    def as_posix(self) -> str:
        return self.uri

    def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        return None

    def exists(self) -> bool:
        client = _s3_client()
        if self.key:
            try:
                client.head_object(Bucket=self.bucket, Key=self.key)
                return True
            except Exception:
                pass
        prefix = self.key.rstrip("/") + "/" if self.key else ""
        response = client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, MaxKeys=1)
        return bool(response.get("Contents"))

    def stat(self) -> Any:
        response = _s3_client().head_object(Bucket=self.bucket, Key=self.key)
        return SimpleNamespace(st_size=int(response.get("ContentLength", 0)))

    def read_bytes(self) -> bytes:
        response = _s3_client().get_object(Bucket=self.bucket, Key=self.key)
        return response["Body"].read()

    def read_text(self, encoding: str = "utf-8") -> str:
        return self.read_bytes().decode(encoding)

    def write_bytes(self, data: bytes) -> None:
        _s3_client().put_object(Bucket=self.bucket, Key=self.key, Body=data)

    def write_text(self, data: str, encoding: str = "utf-8") -> None:
        self.write_bytes(data.encode(encoding))

    def open(self, mode: str = "r", encoding: str | None = None, **_: Any) -> Any:
        encoding = encoding or "utf-8"
        if "b" in mode:
            if "r" in mode:
                return io.BytesIO(self.read_bytes())
            raise ValueError("Binary S3 writes should use write_bytes()")
        if "r" in mode:
            return io.StringIO(self.read_text(encoding=encoding))
        if "w" in mode:
            return _S3TextWriter(self, encoding=encoding)
        raise ValueError(f"Unsupported S3 open mode: {mode}")

    def glob(self, pattern: str) -> Iterator["S3Path"]:
        prefix = self.key.rstrip("/") + "/" if self.key else ""
        for key in self._iter_keys(prefix):
            relative_key = key.removeprefix(prefix)
            if "/" in relative_key:
                continue
            if fnmatch.fnmatch(posixpath.basename(key), pattern):
                yield self._with_key(key)

    def rglob(self, pattern: str) -> Iterator["S3Path"]:
        prefix = self.key.rstrip("/") + "/" if self.key else ""
        for key in self._iter_keys(prefix):
            if fnmatch.fnmatch(posixpath.basename(key), pattern):
                yield self._with_key(key)

    def relative_to(self, other: "S3Path") -> Any:
        if not isinstance(other, S3Path) or self.bucket != other.bucket:
            raise ValueError(f"{self} is not under {other}")
        base = other.key.rstrip("/")
        if base and not self.key.startswith(base + "/"):
            raise ValueError(f"{self} is not under {other}")
        relative_key = self.key.removeprefix(base).lstrip("/")
        from pathlib import PurePosixPath

        return PurePosixPath(relative_key)

    def _iter_keys(self, prefix: str) -> Iterator[str]:
        client = _s3_client()
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            response = client.list_objects_v2(**kwargs)
            for item in response.get("Contents", []):
                key = item.get("Key")
                if key and not key.endswith("/"):
                    yield key
            if not response.get("IsTruncated"):
                break
            token = response.get("NextContinuationToken")
