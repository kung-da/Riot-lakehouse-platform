from __future__ import annotations

import argparse

from lakehouse.common.config import load_config
from lakehouse.quality.data_quality import run_data_quality


def _split_csv(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Silver/Gold data quality checks")
    parser.add_argument(
        "--env",
        default="dev",
        help="Config environment name from configs/<env>.yaml",
    )
    parser.add_argument(
        "--layers",
        help="Comma-separated layers to check. Defaults to quality.layers or silver,gold.",
    )
    parser.add_argument("--silver-tables", help="Comma-separated Silver tables to check")
    parser.add_argument("--gold-tables", help="Comma-separated Gold tables to check")
    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Exit with status 1 when any error-level check fails",
    )
    args = parser.parse_args()

    config = load_config(args.env)
    tables_by_layer = {
        layer: tables
        for layer, tables in {
            "silver": _split_csv(args.silver_tables),
            "gold": _split_csv(args.gold_tables),
        }.items()
        if tables is not None
    }
    result = run_data_quality(
        config,
        layers=_split_csv(args.layers),
        tables_by_layer=tables_by_layer or None,
    )
    report = result["report"]

    print(f"Data quality status: {report['status']}")
    print(f"Recommendation: {report['summary']['recommendation']}")
    print(f"Tables analyzed: {report['summary']['tables_analyzed']}")
    print(f"JSON report: {result['paths']['json']}")
    print(f"Markdown report: {result['paths']['markdown']}")

    if args.fail_on_error and report["status"] == "FAIL":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
