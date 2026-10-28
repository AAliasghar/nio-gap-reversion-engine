"""Guard against files landing at the wrong path (what PR #8 did).

Each check reads a file and confirms it contains the kind of content its
path promises, so a swapped file fails CI instead of breaking `main`.
"""

import ast
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent


def _python_imports(path: Path) -> set:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
    return names


def test_docker_compose_is_a_compose_file():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    assert isinstance(compose, dict)
    assert {"nio_postgres", "airflow_db", "airflow"} <= set(compose["services"])


def test_airflow_service_gets_warehouse_connection_settings():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    env = compose["services"]["airflow"]["environment"]
    names = {item.split("=", 1)[0] for item in env}
    assert {"NIO_DB_HOST", "NIO_DB_PORT", "NIO_DB_NAME", "NIO_DB_USER", "NIO_DB_PASSWORD"} <= names


def test_dbt_profile_is_a_dbt_profile_without_hardcoded_password():
    text = (ROOT / "sql" / "nio_strategies" / "profiles.yml").read_text(encoding="utf-8")
    profile = yaml.safe_load(text)
    dev = profile["nio_strategies"]["outputs"]["dev"]
    assert dev["type"] == "postgres"
    assert "env_var('NIO_DB_PASSWORD')" in dev["password"]


def test_dag_file_defines_the_airflow_dag():
    path = ROOT / "dags" / "nio_dag.py"
    assert "airflow" in _python_imports(path)
    assert '"nio_market_ingestion"' in path.read_text(encoding="utf-8")


def test_scanner_and_transform_are_in_their_own_files():
    scanner = _python_imports(ROOT / "scripts" / "scanner_nio.py")
    transform = _python_imports(ROOT / "scripts" / "transform_nio_spark.py")
    assert "pyspark" not in scanner and "sqlalchemy" in scanner
    assert "pyspark" in transform


def test_no_hardcoded_database_password_in_pipeline_code():
    for path in [ROOT / "dags" / "nio_dag.py", *sorted((ROOT / "scripts").glob("*.py"))]:
        if path.name == "config.py":  # holds the local-dev defaults on purpose
            continue
        assert "quant_password" not in path.read_text(encoding="utf-8"), path
