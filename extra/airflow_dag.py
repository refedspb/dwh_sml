"""Airflow-DAG конвейера: DDL -> Core (DQ + курсы) -> витрина -> ML-скоринг -> (опц.) ClickHouse.

Каждая задача вызывает тот же CLI, что и `docker-compose up`, поэтому результаты идентичны обычному запуску.
Все шаги идемпотентны, а запуски взаимно исключаются advisory lock в БД; `max_active_runs=1` страхует и на уровне Airflow.

Запуск вручную с окном backfill (даты по prices.loading_date, обе границы включительно):
    airflow dags trigger smart_logistics_pipeline \
        --conf '{"start_date": "2026-09-01", "end_date": "2026-09-30"}'
Без параметров загружается весь источник. Для боевого расписания достаточно заменить `schedule=None`
на `@daily` и подставить в окно {{ ds }} (см. README).
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"
PYTHON = "/opt/airflow/venv/bin/python"          # отдельное окружение с зависимостями проекта (см. docker/airflow.Dockerfile)

# Параметры подставляются в bash-команды, поэтому строго ограничены форматом даты (защита от инъекций).
DATE_PATTERN = r"^(\d{4}-\d{2}-\d{2})?$"

WINDOW = (
    "{% if params.start_date %}--start-date {{ params.start_date }}{% endif %} "
    "{% if params.end_date %}--end-date {{ params.end_date }}{% endif %}"
)
AS_OF = "{% if params.as_of %}--as-of {{ params.as_of }}{% endif %}"


def _task(task_id: str, command: str, **kwargs) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=f"cd {PROJECT_DIR} && {command}", **kwargs)


with DAG(
    dag_id="smart_logistics_pipeline",
    description="ETL -> Core DWH -> mart_route_card -> CatBoost scoring (-> ClickHouse)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "start_date": Param("", type="string", pattern=DATE_PATTERN, description="Окно ETL: нижняя граница loading_date"),
        "end_date": Param("", type="string", pattern=DATE_PATTERN, description="Окно ETL: верхняя граница loading_date"),
        "as_of": Param("", type="string", pattern=DATE_PATTERN, description="Дата снимка (по умолчанию AS_OF из окружения)"),
    },
    default_args={"retries": 1},
    tags=["logistics", "etl", "ml"],
) as dag:
    init_ddl = _task("init_ddl", f"{PYTHON} -m etl.pipeline init")
    load_core = _task("load_core", f"{PYTHON} -m etl.pipeline core {WINDOW}")
    refresh_marts = _task("refresh_marts", f"{PYTHON} -m etl.pipeline marts {AS_OF}")
    score_ml = _task("score_ml", f"{PYTHON} -m ml.train_score")
    # Код 99 = «пропустить задачу» (skip_on_exit_code): ClickHouse включается переменной ENABLE_CLICKHOUSE_SYNC=1.
    sync_clickhouse = _task(
        "sync_clickhouse",
        f'[ "${{ENABLE_CLICKHOUSE_SYNC:-0}}" = "1" ] || exit 99; {PYTHON} -m extra.clickhouse_sync',
    )

    init_ddl >> load_core >> refresh_marts >> score_ml >> sync_clickhouse
