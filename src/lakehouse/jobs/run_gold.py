from __future__ import annotations

import argparse

from lakehouse.common.config import load_config
from lakehouse.gold.gold_transformer import GOLD_TABLES, run_gold_transform


def _split_csv(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Gold analytics aggregates")
    parser.add_argument("--env", default="dev", help="Config environment name from configs/<env>.yaml")
    parser.add_argument(
        "--tables",
        help=f"Comma-separated Gold tables to write. Available: {', '.join(GOLD_TABLES)}",
    )
    parser.add_argument(
        "--write-mode",
        choices=["append", "overwrite", "ignore", "error", "errorifexists"],
        help="Spark write mode for Gold tables. Defaults to gold.write_mode or overwrite.",
    )
    args = parser.parse_args()

    config = load_config(args.env)
    counts = run_gold_transform(
        config,
        tables=_split_csv(args.tables),
        write_mode=args.write_mode,
    )
    print("Gold transform completed:")
    for table, count in counts.items():
        print(f"{table}: {count}")


if __name__ == "__main__":
    main()
