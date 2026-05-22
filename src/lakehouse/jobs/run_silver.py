from __future__ import annotations

import argparse

from lakehouse.common.config import load_config
from lakehouse.silver.silver_transformer import SILVER_TABLES, run_silver_transform


def _split_csv(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Silver transforms")
    parser.add_argument("--env", default="dev", help="Config environment name from configs/<env>.yaml")
    parser.add_argument(
        "--datasets",
        help="Comma-separated Bronze datasets to transform, e.g. matches,timelines",
    )
    parser.add_argument(
        "--tables",
        help=f"Comma-separated Silver tables to write. Available: {', '.join(SILVER_TABLES)}",
    )
    parser.add_argument(
        "--write-mode",
        choices=["append", "overwrite", "ignore", "error", "errorifexists"],
        help="Spark write mode for Silver tables. Defaults to silver.write_mode or overwrite.",
    )
    args = parser.parse_args()

    config = load_config(args.env)
    counts = run_silver_transform(
        config,
        datasets=_split_csv(args.datasets),
        tables=_split_csv(args.tables),
        write_mode=args.write_mode,
    )
    print(f"Silver transform completed: {counts}")


if __name__ == "__main__":
    main()
