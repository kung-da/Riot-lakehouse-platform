import importlib


def test_dag_modules_import_without_airflow():
    for module in [
        "dags.bronze_ingestion_dag",
        "dags.silver_transform_dag",
        "dags.gold_aggregation_dag",
        "dags.platinum_feature_dag",
        "dags.full_lakehouse_pipeline_dag",
    ]:
        importlib.import_module(module)
