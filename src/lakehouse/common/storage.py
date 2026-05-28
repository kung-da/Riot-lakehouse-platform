from __future__ import annotations

import fnmatch
import io
import os
import posixpath
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Iterator
from urllib.parse import urlsplit


S3_SCHEMES = ("s3://", "s3a://")


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

    profile = os.getenv("AWS_PROFILE") or None
    region_name = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or None
    endpoint_url = os.getenv("AWS_S3_ENDPOINT_URL") or os.getenv("AWS_ENDPOINT_URL") or None
    session = boto3.Session(profile_name=profile, region_name=region_name)
    return session.client("s3", endpoint_url=endpoint_url)


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
