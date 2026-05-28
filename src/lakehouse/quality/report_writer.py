from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from lakehouse.common.storage import S3Path


def write_quality_reports(
    output_dir: Path | S3Path, report: dict[str, Any]
) -> dict[str, Path | S3Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = str(report["run_id"])

    json_path = output_dir / f"data_quality_{run_id}.json"
    markdown_path = output_dir / f"data_quality_{run_id}.md"
    latest_json_path = output_dir / "data_quality_latest.json"
    latest_markdown_path = output_dir / "data_quality_latest.md"

    _write_json(json_path, report)
    _write_json(latest_json_path, report)

    markdown = render_markdown_report(report)
    markdown_path.write_text(markdown, encoding="utf-8")
    latest_markdown_path.write_text(markdown, encoding="utf-8")

    return {
        "json": json_path,
        "markdown": markdown_path,
        "latest_json": latest_json_path,
        "latest_markdown": latest_markdown_path,
    }


def render_markdown_report(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# Riot Lakehouse Data Quality Report",
        "",
        f"- Run ID: `{report['run_id']}`",
        f"- Generated at: `{report['generated_at']}`",
        f"- Environment: `{report['environment']}`",
        f"- Status: **{report['status']}**",
        f"- Dashboard ready: **{str(summary['ready_for_dashboard']).lower()}**",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Tables expected | {summary['tables_expected']} |",
        f"| Tables analyzed | {summary['tables_analyzed']} |",
        f"| Missing tables | {summary['missing_tables']} |",
        f"| Passed tables | {summary['passed_tables']} |",
        f"| Warning tables | {summary['warning_tables']} |",
        f"| Failed tables | {summary['failed_tables']} |",
        "",
        "## Table Status",
        "",
        "| Layer | Table | Status | Rows | Date Range |",
        "| --- | --- | --- | ---: | --- |",
    ]

    for table in report["tables"]:
        date_range = _date_range_text(table.get("profile", {}))
        lines.append(
            "| {layer} | {table} | {status} | {rows} | {date_range} |".format(
                layer=table["layer"],
                table=table["table"],
                status=table["status"],
                rows=table.get("row_count", 0),
                date_range=date_range,
            )
        )

    issues = _issue_rows(report)
    lines.extend(["", "## Issues", ""])
    if issues:
        lines.extend(
            [
                "| Layer | Table | Check | Status | Failed Rows | Detail |",
                "| --- | --- | --- | --- | ---: | --- |",
            ]
        )
        for issue in issues:
            lines.append(
                "| {layer} | {table} | {check} | {status} | {failed_rows} | {detail} |".format(
                    layer=issue["layer"],
                    table=issue["table"],
                    check=issue["check"],
                    status=issue["status"],
                    failed_rows=issue["failed_rows"],
                    detail=issue["detail"],
                )
            )
    else:
        lines.append("No failed or warning checks.")

    lines.extend(["", "## Profiles", ""])
    for table in report["tables"]:
        profile = table.get("profile", {})
        lines.extend(
            [
                f"### {table['layer']}.{table['table']}",
                "",
                f"- Rows: `{profile.get('row_count', table.get('row_count', 0))}`",
                f"- Columns: `{profile.get('column_count', 0)}`",
                f"- Checks: `{_check_summary(table.get('checks', []))}`",
                "",
            ]
        )

    return "\n".join(lines).rstrip() + "\n"


def _write_json(path: Path | S3Path, report: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, sort_keys=True)


def _date_range_text(profile: dict[str, Any]) -> str:
    coverage = profile.get("game_date_coverage") or {}
    min_date = coverage.get("min")
    max_date = coverage.get("max")
    if not min_date and not max_date:
        return "-"
    if min_date == max_date:
        return str(min_date)
    return f"{min_date} to {max_date}"


def _issue_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for table in report["tables"]:
        for check in table.get("checks", []):
            if check["status"] not in {"FAIL", "WARN"}:
                continue
            rows.append(
                {
                    "layer": table["layer"],
                    "table": table["table"],
                    "check": check["name"],
                    "status": check["status"],
                    "failed_rows": check.get("failed_rows", ""),
                    "detail": _compact_detail(check.get("details", {})),
                }
            )
    return rows


def _compact_detail(details: dict[str, Any]) -> str:
    if not details:
        return ""
    preferred_keys = [
        "missing_columns",
        "key_columns",
        "accepted_values",
        "invalid_rate",
        "top_invalid_values",
        "checked_distinct_keys",
        "invalid_distinct_keys",
        "invalid_reference_count",
        "row_count",
        "column_count",
        "min_count",
        "max_count",
        "target",
        "delta_rate",
    ]
    compact = {key: details[key] for key in preferred_keys if key in details}
    if not compact:
        compact = details
    return json.dumps(compact, sort_keys=True)


def _check_summary(checks: list[dict[str, Any]]) -> str:
    counts = {"PASS": 0, "WARN": 0, "FAIL": 0, "SKIP": 0}
    for check in checks:
        counts[check["status"]] = counts.get(check["status"], 0) + 1
    return ", ".join(f"{status}: {count}" for status, count in counts.items() if count)
