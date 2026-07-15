import os
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from marketing_api.utils.paths import build_success_marker_path


class WriteSuccessMarkerOperator(BaseOperator):
    # маркер _SUCCESS — пишем только если файл реально скачался и не пустой
    template_fields = ("data_base_dir", "ds")

    def __init__(
        self,
        *,
        data_base_dir: str,
        ds: str,
        download_task_id: str = "download_result",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.data_base_dir = data_base_dir
        self.ds = ds
        self.download_task_id = download_task_id

    def execute(self, context: Context) -> str:
        file_size = context["ti"].xcom_pull(
            task_ids=self.download_task_id,
            key="file_size",
        )
        if not file_size or int(file_size) <= 0:
            raise AirflowException("Refusing to write _SUCCESS for empty export")

        marker_path = build_success_marker_path(self.data_base_dir, self.ds)
        os.makedirs(os.path.dirname(marker_path), exist_ok=True)
        with open(marker_path, "w", encoding="utf-8") as fh:
            fh.write("")
        self.log.info("Created success marker %s", marker_path)
        return marker_path
