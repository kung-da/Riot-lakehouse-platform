"""Test AWS connectivity using credentials from a local env file."""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Any, Callable

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    print("[FAIL] boto3 is not installed. Run: python -m pip install boto3")
    sys.exit(1)


def load_env_file(path: Path) -> None:
    if not path.exists():
        print(f"[WARN] env file not found: {path}")
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if value:
            os.environ[key] = value
        else:
            os.environ.pop(key, None)


def mask_account(account: str | None) -> str:
    if not account:
        return "<unknown>"
    return "*" * max(0, len(account) - 4) + account[-4:]


def principal_type(arn: str | None) -> str:
    if not arn:
        return "<unknown>"
    resource = arn.split(":", 5)[-1]
    return resource.split("/", 1)[0]


def redact_aws_message(message: str) -> str:
    message = re.sub(r"arn:aws:[^\s]+", "<aws-arn>", message)
    return re.sub(r"\b\d{12}\b", lambda match: mask_account(match.group(0)), message)


def run_check(name: str, check: Callable[[], str]) -> bool:
    try:
        print(f"[OK] {name}: {check()}")
        return True
    except ClientError as exc:
        error = exc.response.get("Error", {})
        message = redact_aws_message(str(error.get("Message", exc)))
        print(f"[FAIL] {name}: {error.get('Code')} - {message}")
    except BotoCoreError as exc:
        print(f"[FAIL] {name}: {type(exc).__name__} - {exc}")
    except Exception as exc:  # noqa: BLE001 - this is a diagnostic CLI.
        print(f"[FAIL] {name}: {type(exc).__name__} - {exc}")
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Test AWS STS, S3, and Glue connectivity.")
    parser.add_argument("--env-file", default=".env", help="Path to the env file to load.")
    args = parser.parse_args()

    load_env_file(Path(args.env_file))

    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1"
    profile = os.getenv("AWS_PROFILE") or None
    bucket = os.getenv("S3_BUCKET")
    database = os.getenv("ATHENA_DATABASE") or "riot_lakehouse"
    endpoint_url = os.getenv("AWS_S3_ENDPOINT_URL") or None

    print(f"region={region}")
    print(f"s3_bucket={bucket or '<missing>'}")
    print(f"glue_database={database}")

    session = boto3.Session(profile_name=profile, region_name=region)
    ok = True

    def check_identity() -> str:
        response: dict[str, Any] = session.client("sts", region_name=region).get_caller_identity()
        return (
            f"account={mask_account(response.get('Account'))}, "
            f"principal_type={principal_type(response.get('Arn'))}"
        )

    ok &= run_check("sts.get_caller_identity", check_identity)

    if bucket:
        s3 = session.client("s3", region_name=region, endpoint_url=endpoint_url)
        ok &= run_check(
            "s3.head_bucket",
            lambda: (s3.head_bucket(Bucket=bucket), "bucket reachable")[1],
        )
        ok &= run_check(
            "s3.list_objects_v2",
            lambda: f"sample_keys={s3.list_objects_v2(Bucket=bucket, MaxKeys=1).get('KeyCount', 0)}",
        )
    else:
        print("[SKIP] s3: S3_BUCKET is missing")
        ok = False

    glue = session.client("glue", region_name=region)
    ok &= run_check(
        "glue.get_database",
        lambda: f"database={glue.get_database(Name=database).get('Database', {}).get('Name')}",
    )

    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
