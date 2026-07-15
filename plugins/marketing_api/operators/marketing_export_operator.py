from typing import Any, Optional

from airflow.models import BaseOperator
from airflow.utils.context import Context

from marketing_api.hooks.marketing_api_hook import MarketingApiHook
from marketing_api.utils.config_resolver import resolve
from marketing_api.utils.state_store import StateStore


class MarketingExportOperator(BaseOperator):
    # запускает выгрузку и кладёт job_id в XCom; ждать готовности не должен — это Sensor
    template_fields = ("date_from", "date_to", "output_path", "updated_after")

    def __init__(
        self,
        *,
        date_from: str,
        date_to: str,
        output_path: str,
        export_mode: str = "full",
        updated_after: Optional[str] = None,
        marketing_conn_id: str = MarketingApiHook.default_conn_name,
        export_format: Optional[str] = None,
        max_page_size: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.date_from = date_from
        self.date_to = date_to
        self.output_path = output_path
        self.export_mode = export_mode
        self.updated_after = updated_after
        self.marketing_conn_id = marketing_conn_id
        self.export_format = export_format
        self.max_page_size = max_page_size

    def execute(self, context: Context) -> str:
        hook = MarketingApiHook(marketing_conn_id=self.marketing_conn_id)
        extra = hook.conn_extra
        export_format = resolve(
            "default_format",
            self.export_format,
            extra,
            "jsonl",
        )
        max_page_size = resolve(
            "max_page_size",
            self.max_page_size,
            extra,
            None,
        )

        updated_after = self.updated_after
        # для incremental, если дату не передали явно — берём из StateStore
        if self.export_mode == "incremental" and not updated_after:
            store = StateStore()
            last_ts = store.get_last_successful_ts(
                context["dag"].dag_id,
                self.task_id,
            )
            updated_after = last_ts or self.date_from
            self.log.info(
                "Incremental export updated_after=%s (last_ts=%s)",
                updated_after,
                last_ts,
            )

        self.log.info(
            "Starting export date_from=%s date_to=%s mode=%s path=%s "
            "conn_id=%s format=%s max_page_size=%s",
            self.date_from,
            self.date_to,
            self.export_mode,
            self.output_path,
            self.marketing_conn_id,
            export_format,
            max_page_size,
        )

        job_id = hook.start_export(
            date_from=self.date_from,
            date_to=self.date_to,
            export_format=export_format,
            mode=self.export_mode,
            updated_after=updated_after,
            max_page_size=int(max_page_size) if max_page_size is not None else None,
        )

        context["ti"].xcom_push(key="job_id", value=job_id)
        context["ti"].xcom_push(key="export_mode", value=self.export_mode)
        context["ti"].xcom_push(key="output_path", value=self.output_path)
        return job_id
