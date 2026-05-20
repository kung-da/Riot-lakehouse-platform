from lakehouse.jobs._cli import load_job_config


def main() -> None:
    load_job_config("Run Silver transforms")
    print("Silver transform scaffold ready. Wire Spark IO after Bronze landing is finalized.")


if __name__ == "__main__":
    main()
