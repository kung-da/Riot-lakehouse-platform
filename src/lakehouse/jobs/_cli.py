from __future__ import annotations

import argparse

from lakehouse.common.config import load_config


def load_job_config(description: str):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--env", default="dev", help="Config environment name from configs/<env>.yaml")
    args = parser.parse_args()
    return load_config(args.env)
