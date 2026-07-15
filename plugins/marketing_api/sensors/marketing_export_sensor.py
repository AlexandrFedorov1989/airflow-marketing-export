from typing import Any, Optional

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from marketing_api.hooks.marketing_api_hook import MarketingApiHook
from marketing_api.utils.config_resolver import resolve


class MarketingExportSensor(BaseSensorOperator):
    # ждём completed; failed/cancelled — сразу падаем
    template_fields = ("export_task_id",)

    def __init__(
        self,
        *,
        export_task_id: str = "start_export",
        marketing_conn_id: str = MarketingApiHook.default_conn_name,
        poke_interval: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        # если poke_interval не передали в оператор — возьмём из Extra уже в poke()
        self._poke_interval_from_operator = poke_interval
        if poke_interval is not None:
            kwargs["poke_interval"] = poke_interval
        super().__init__(**kwargs)
        self.export_task_id = export_task_id
        self.marketing_conn_id = marketing_conn_id

    def poke(self, context: Context) -> bool:
        if self._poke_interval_from_operator is None:
            hook = MarketingApiHook(marketing_conn_id=self.marketing_conn_id)
            self.poke_interval = int(
                resolve("poll_interval", None, hook.conn_extra, 30)
            )

        job_id = context["ti"].xcom_pull(task_ids=self.export_task_id, key="job_id")
        if not job_id:
            raise AirflowException(f"No job_id from task {self.export_task_id}")

        hook = MarketingApiHook(marketing_conn_id=self.marketing_conn_id)
        status_payload = hook.get_export_status(job_id)
        status = status_payload.get("status")
        self.log.info("Export job_id=%s status=%s", job_id, status)

        if status == "completed":
            return True
        if status in ("failed", "cancelled"):
            raise AirflowException(
                f"Export job {job_id} failed: {status_payload.get('error_message')}"
            )
        return False
