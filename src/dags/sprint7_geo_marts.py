"""Ежедневный расчёт витрин по последним доступным полным суткам UTC."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="sprint7_geo_marts",
    description="Геоаналитика и рекомендации друзей",
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["sprint7", "data-lake"],
) as dag:
    for mart in ("user", "zone", "friend"):
        BashOperator(
            task_id=f"build_{mart}_mart",
            bash_command=(
                'args=(--as-of "$RUN_DATE" --output-root "$OUTPUT_ROOT" '
                '--events-path "$EVENTS_PATH" --cities-path "$CITIES_PATH"); '
                'if [ -n "$HISTORY_START" ]; then args+=(--history-start "$HISTORY_START"); fi; '
                'spark-submit --master "$SPARK_MASTER" '
                f'"/lessons/scripts/{mart}_mart.py" "${{args[@]}}"'
            ),
            env={
                "RUN_DATE": "{{ dag_run.conf.get('as_of') or data_interval_end.subtract(days=1).to_date_string() }}",
                "HISTORY_START": "{{ dag_run.conf.get('history_start') or '' }}",
                "OUTPUT_ROOT": "{{ dag_run.conf.get('output_root') or var.value.get('sprint7_output_root', '/user/master/data/geo/sprint7/analytics') }}",
                "EVENTS_PATH": "{{ var.value.get('sprint7_events_path', '/user/master/data/geo/events') }}",
                "CITIES_PATH": "{{ var.value.get('sprint7_cities_path', '/user/master/data/geo/sprint7/reference/geo.csv') }}",
                "SPARK_MASTER": "{{ var.value.get('sprint7_spark_master', 'yarn') }}",
            },
            append_env=True,
        )
