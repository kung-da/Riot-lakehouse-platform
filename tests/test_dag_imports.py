import importlib
import sys
import types


DAG_MODULES = [
    "dags.bronze_ingestion_dag",
    "dags.silver_transform_dag",
    "dags.gold_aggregation_dag",
    "dags.platinum_feature_dag",
    "dags.full_lakehouse_pipeline_dag",
]


class FakeDAG:
    current = None

    def __init__(self, dag_id, **kwargs):
        self.dag_id = dag_id
        self.kwargs = kwargs
        self.tasks = []

    def __enter__(self):
        FakeDAG.current = self
        return self

    def __exit__(self, exc_type, exc, traceback):
        FakeDAG.current = None
        return False


class FakeBashOperator:
    def __init__(self, task_id, bash_command, **kwargs):
        self.task_id = task_id
        self.bash_command = bash_command
        self.kwargs = kwargs
        self.upstream_task_ids = set()
        self.downstream_task_ids = set()
        if FakeDAG.current is not None:
            FakeDAG.current.tasks.append(self)

    def __rshift__(self, other):
        self.downstream_task_ids.add(other.task_id)
        other.upstream_task_ids.add(self.task_id)
        return other


def _install_fake_airflow(monkeypatch):
    airflow_module = types.ModuleType("airflow")
    airflow_module.DAG = FakeDAG
    airflow_operators_module = types.ModuleType("airflow.operators")
    airflow_bash_module = types.ModuleType("airflow.operators.bash")
    airflow_bash_module.BashOperator = FakeBashOperator
    monkeypatch.setitem(sys.modules, "airflow", airflow_module)
    monkeypatch.setitem(sys.modules, "airflow.operators", airflow_operators_module)
    monkeypatch.setitem(sys.modules, "airflow.operators.bash", airflow_bash_module)


def _fresh_import(module_name):
    for module in ["dags._common", *DAG_MODULES]:
        sys.modules.pop(module, None)
    return importlib.import_module(module_name)


def test_dag_modules_import_without_airflow():
    for module in DAG_MODULES:
        importlib.import_module(module)


def test_single_task_dags_with_airflow(monkeypatch):
    _install_fake_airflow(monkeypatch)

    expected = {
        "dags.bronze_ingestion_dag": (
            "riot_bronze_ingestion",
            "run_bronze",
            "python -m lakehouse.jobs.run_bronze",
        ),
        "dags.silver_transform_dag": (
            "riot_silver_transform",
            "run_silver",
            "python -m lakehouse.jobs.run_silver",
        ),
        "dags.gold_aggregation_dag": (
            "riot_gold_model",
            "run_gold",
            "python -m lakehouse.jobs.run_gold",
        ),
        "dags.platinum_feature_dag": (
            "riot_platinum_features",
            "run_platinum",
            "python -m lakehouse.jobs.run_platinum",
        ),
    }

    for module_name, (dag_id, task_id, command) in expected.items():
        module = _fresh_import(module_name)
        assert module.dag.dag_id == dag_id
        assert module.dag.kwargs["catchup"] is False
        assert module.dag.kwargs["schedule"] is None
        assert len(module.dag.tasks) == 1
        assert module.dag.tasks[0].task_id == task_id
        assert module.dag.tasks[0].bash_command == command


def test_full_dag_task_order_and_commands_with_airflow(monkeypatch):
    _install_fake_airflow(monkeypatch)

    module = _fresh_import("dags.full_lakehouse_pipeline_dag")
    dag = module.dag
    tasks = {task.task_id: task for task in dag.tasks}

    assert list(tasks) == [
        "run_bronze",
        "run_silver",
        "run_gold",
        "run_platinum",
        "run_data_quality",
    ]
    assert tasks["run_bronze"].bash_command == "python -m lakehouse.jobs.run_bronze"
    assert tasks["run_silver"].bash_command == "python -m lakehouse.jobs.run_silver"
    assert tasks["run_gold"].bash_command == "python -m lakehouse.jobs.run_gold"
    assert tasks["run_platinum"].bash_command == "python -m lakehouse.jobs.run_platinum"
    assert tasks["run_data_quality"].bash_command == (
        "python -m lakehouse.jobs.run_data_quality"
    )
    assert tasks["run_bronze"].downstream_task_ids == {"run_silver"}
    assert tasks["run_silver"].downstream_task_ids == {"run_gold"}
    assert tasks["run_gold"].downstream_task_ids == {"run_platinum"}
    assert tasks["run_platinum"].downstream_task_ids == {"run_data_quality"}
