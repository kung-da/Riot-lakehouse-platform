from lakehouse.jobs._cli import load_job_config


def main() -> None:
    config = load_job_config("Run data validation")
    print(f"Validation report root: {config.report_root}")


if __name__ == "__main__":
    main()
