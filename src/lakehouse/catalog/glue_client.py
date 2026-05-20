def get_glue_client(region_name: str = "ap-southeast-1"):
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("Install the aws extra to use Glue: pip install -e '.[aws]'") from exc
    return boto3.client("glue", region_name=region_name)
