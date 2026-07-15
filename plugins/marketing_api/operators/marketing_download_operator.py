from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from marketing_api.hooks.marketing_api_hook import MarketingApiHook


class MarketingDownloadOperator(BaseOperator):
    # скачивает файл по job_id из XCom; сам HTTP не пишет — только вызывает Hook
    template_fields = ("output_path",)

    def __init__(
        self,
        *,
        export_task_id: str = "start_export",
        output_path: str = "",
        marketing_conn_id: str = MarketingApiHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.export_task_id = export_task_id
        self.output_path = output_path
        self.marketing_conn_id = marketing_conn_id

    def execute(self, context: Context) -> int:
        ti = context["ti"]
        job_id = ti.xcom_pull(task_ids=self.export_task_id, key="job_id")
        if not job_id:
            raise AirflowException(f"No job_id in XCom from task {self.export_task_id}")

        dest_path = self.output_path or ti.xcom_pull(
            task_ids=self.export_task_id,
            key="output_path",
        )
        if not dest_path:
            raise AirflowException("output_path is not set")

        hook = MarketingApiHook(marketing_conn_id=self.marketing_conn_id)
        written = hook.download_export_result(job_id, dest_path)
        if written <= 0:
            raise AirflowException(f"Downloaded empty file for job_id={job_id}")

        ti.xcom_push(key="output_path", value=dest_path)
        ti.xcom_push(key="file_size", value=written)
        return written
