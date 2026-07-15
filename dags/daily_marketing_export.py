# DAG полной выгрузки marketing events за день
import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

from marketing_api.hooks.marketing_api_hook import MarketingApiHook
from marketing_api.operators.marketing_download_operator import MarketingDownloadOperator
from marketing_api.operators.marketing_export_operator import MarketingExportOperator
from marketing_api.operators.write_success_marker_operator import WriteSuccessMarkerOperator
from marketing_api.sensors.marketing_export_sensor import MarketingExportSensor
from marketing_api.utils.paths import build_export_path
from marketing_api.utils.state_store import StateStore

DATA_BASE_DIR = os.environ.get(
    "MARKETING_DATA_DIR",
    os.path.join(os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow")), "data"),
)
MARKETING_CONN_ID = "marketing_api_default"

default_args = {
    "owner": "marketing-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def validate_connection_callable() -> None:
    hook = MarketingApiHook(marketing_conn_id=MARKETING_CONN_ID)
    if not hook.healthcheck():
        raise AirflowException("Marketing API healthcheck failed")


with DAG(
    dag_id="daily_marketing_export",
    default_args=default_args,
    description="Daily marketing events export (full)",
    schedule="@daily",
    start_date=datetime(2026, 6, 10),
    # без catchup, иначе при включении нагонит кучу старых дат
    catchup=False,
    max_active_runs=1,
    tags=["marketing", "export"],
) as dag:
    validate_connection = PythonOperator(
        task_id="validate_connection",
        python_callable=validate_connection_callable,
    )

    start_export = MarketingExportOperator(
        task_id="start_export",
        date_from="{{ ds }}",
        date_to="{{ ds }}",
        export_mode="full",
        output_path=build_export_path(DATA_BASE_DIR, "{{ ds }}"),
        marketing_conn_id=MARKETING_CONN_ID,
    )

    # reschedule — между опросами воркер свободен (выгрузка может идти долго)
    wait_export_ready = MarketingExportSensor(
        task_id="wait_export_ready",
        export_task_id="start_export",
        marketing_conn_id=MARKETING_CONN_ID,
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )

    download_result = MarketingDownloadOperator(
        task_id="download_result",
        export_task_id="start_export",
        output_path=build_export_path(DATA_BASE_DIR, "{{ ds }}"),
        marketing_conn_id=MARKETING_CONN_ID,
    )

    def verify_file_callable(**context) -> int:
        # проверяем что файл есть, не пустой и строки — нормальный JSONL
        output_path = context["ti"].xcom_pull(
            task_ids="download_result",
            key="output_path",
        )
        if not output_path:
            raise AirflowException("output_path missing from download_result XCom")
        if not os.path.exists(output_path):
            raise AirflowException(f"Export file not found: {output_path}")
        size = os.path.getsize(output_path)
        if size <= 0:
            raise AirflowException(f"Export file is empty: {output_path}")

        with open(output_path, "r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    json.loads(line)
                except json.JSONDecodeError as exc:
                    raise AirflowException(
                        f"Invalid JSONL at line {line_no}: {exc}"
                    ) from exc
        return size

    verify_file = PythonOperator(
        task_id="verify_file",
        python_callable=verify_file_callable,
    )

    write_success_marker = WriteSuccessMarkerOperator(
        task_id="write_success_marker",
        data_base_dir=DATA_BASE_DIR,
        ds="{{ ds }}",
        download_task_id="download_result",
    )

    @task(task_id="update_last_successful_ts")
    def update_last_successful_ts() -> str:
        store = StateStore()
        store.set_last_successful_ts(
            dag_id="daily_marketing_export",
            task_id="start_export",
        )
        return datetime.utcnow().isoformat()

    update_state = update_last_successful_ts()

    validate_connection >> start_export >> wait_export_ready >> download_result
    download_result >> verify_file >> write_success_marker >> update_state
