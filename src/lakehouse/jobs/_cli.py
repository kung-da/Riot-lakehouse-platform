from __future__ import annotations

import argparse

from lakehouse.common.config import load_config


def add_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--env",
        default=None,
        help="Config environment name or YAML path. Defaults to LAKEHOUSE_ENV or dev.",
    )
    parser.add_argument(
        "--config-dir",
        default=None,
        help="Directory with <env>.yaml configs. Defaults to LAKEHOUSE_CONFIG_DIR or configs.",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional .env file to load before resolving config values.",
    )


def load_config_from_args(args: argparse.Namespace):
    return load_config(
        env=args.env,
        config_dir=args.config_dir,
        env_file=args.env_file,
    )


def load_job_config(description: str):
    parser = argparse.ArgumentParser(description=description)
    add_config_args(parser)
    args = parser.parse_args()
    return load_config_from_args(args)
