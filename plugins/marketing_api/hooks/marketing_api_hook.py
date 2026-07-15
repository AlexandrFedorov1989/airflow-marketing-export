import json
import time
from typing import Any, Optional
from urllib.parse import urljoin

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from marketing_api.utils.config_resolver import resolve


class MarketingApiHook(BaseHook):
    # весь HTTP к API живёт здесь, операторы сами requests не дергают
    conn_name_attr = "marketing_conn_id"
    default_conn_name = "marketing_api_default"
    conn_type = "http"
    hook_name = "Marketing API"

    def __init__(
        self,
        marketing_conn_id: str = default_conn_name,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.marketing_conn_id = marketing_conn_id
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self._conn_extra: Optional[dict] = None
        self._base_url: Optional[str] = None

    def _load_conn(self) -> None:
        if self._base_url is not None:
            return
        conn = self.get_connection(self.marketing_conn_id)
        schema = conn.schema or "http"
        host = conn.host or "127.0.0.1"
        if conn.port:
            host = f"{host}:{conn.port}"
        self._base_url = f"{schema}://{host}"
        self._conn_extra = conn.extra_dejson or {}

    @property
    def conn_extra(self) -> dict:
        self._load_conn()
        return self._conn_extra or {}

    def _build_url(self, endpoint: str, *, absolute: bool = False) -> str:
        self._load_conn()
        path = endpoint.lstrip("/")
        if absolute:
            return urljoin(f"{self._base_url}/", path)
        api_version = resolve("api_version", None, self.conn_extra, "v1")
        if not path.startswith("api/"):
            path = f"api/{api_version}/{path}"
        return urljoin(f"{self._base_url}/", path)

    def get_max_page_size(self) -> Optional[int]:
        value = resolve("max_page_size", None, self.conn_extra, None)
        return int(value) if value is not None else None

    def _auth_headers(self) -> dict:
        conn = self.get_connection(self.marketing_conn_id)
        headers = {"Accept": "application/json"}
        extra = conn.extra_dejson or {}
        token = extra.get("token")
        if token:
            headers["Authorization"] = f"Bearer {token}"
        elif conn.login:
            # логин/пароль передаём через auth у requests ниже
            pass
        return headers

    def _get_timeout(self) -> int:
        return int(resolve("timeout", None, self.conn_extra, 30))

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        json_body: Optional[dict] = None,
        expect_json: bool = True,
        stream: bool = False,
        absolute: bool = False,
    ) -> Any:
        # единая точка для всех запросов: ретраи, логи, разбор ошибок
        self._load_conn()
        url = self._build_url(endpoint, absolute=absolute)
        timeout = self._get_timeout()
        conn = self.get_connection(self.marketing_conn_id)
        auth = (conn.login, conn.password) if conn.login else None
        headers = self._auth_headers()
        verify = bool(resolve("verify_ssl", None, self.conn_extra, True))

        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            started = time.monotonic()
            try:
                self.log.info(
                    "HTTP %s %s attempt=%s conn_id=%s",
                    method,
                    url,
                    attempt,
                    self.marketing_conn_id,
                )
                response = requests.request(
                    method=method,
                    url=url,
                    json=json_body,
                    headers=headers,
                    auth=auth,
                    timeout=timeout,
                    verify=verify,
                    stream=stream,
                )
                elapsed_ms = int((time.monotonic() - started) * 1000)
                self.log.info(
                    "HTTP %s %s status=%s elapsed_ms=%s attempt=%s",
                    method,
                    url,
                    response.status_code,
                    elapsed_ms,
                    attempt,
                )

                # 429 и 500 — пробуем ещё раз с backoff
                if response.status_code == 429 and attempt < self.max_retries:
                    sleep_s = self.backoff_factor * (2 ** (attempt - 1))
                    self.log.warning("Rate limited (429), retry in %ss", sleep_s)
                    time.sleep(sleep_s)
                    continue

                if response.status_code >= 500 and attempt < self.max_retries:
                    sleep_s = self.backoff_factor * (2 ** (attempt - 1))
                    self.log.warning("Server error %s, retry in %ss", response.status_code, sleep_s)
                    time.sleep(sleep_s)
                    continue

                response.raise_for_status()

                if stream:
                    return response

                # пустой ответ при ожидании JSON — сразу ошибка
                if not response.content:
                    if expect_json:
                        raise AirflowException(f"Empty response from {url}")
                    return None

                if not expect_json:
                    return response.content

                try:
                    return response.json()
                except json.JSONDecodeError as exc:
                    raise AirflowException(f"Invalid JSON from {url}: {exc}") from exc

            except requests.Timeout as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    raise AirflowException(f"Timeout calling {url}") from exc
                time.sleep(self.backoff_factor * (2 ** (attempt - 1)))
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    raise AirflowException(f"Request failed for {url}: {exc}") from exc
                time.sleep(self.backoff_factor * (2 ** (attempt - 1)))

        raise AirflowException(f"Request failed for {url}: {last_error}")

    def healthcheck(self) -> bool:
        try:
            data = self._request("GET", "health", absolute=True)
            ok = isinstance(data, dict) and data.get("status") == "ok"
            if not ok:
                self.log.error("Healthcheck failed: unexpected payload=%s", data)
            return ok
        except Exception as exc:
            self.log.error("Healthcheck failed: %s", exc)
            return False

    def test_connection(self) -> tuple[bool, str]:
        # кнопка Test в UI Connection — по сути тот же healthcheck
        if self.healthcheck():
            return True, "Connection ok"
        return False, "Healthcheck failed"

    def start_export(
        self,
        date_from: str,
        date_to: str,
        export_format: str = "jsonl",
        mode: str = "full",
        updated_after: Optional[str] = None,
        max_page_size: Optional[int] = None,
    ) -> str:
        payload = {
            "date_from": date_from,
            "date_to": date_to,
            "format": export_format,
            "mode": mode,
        }
        if updated_after:
            payload["updated_after"] = updated_after

        page_size = resolve(
            "max_page_size",
            max_page_size,
            self.conn_extra,
            None,
        )
        if page_size is not None:
            payload["max_page_size"] = int(page_size)

        data = self._request("POST", "exports", json_body=payload)
        job_id = data.get("job_id")
        if not job_id:
            raise AirflowException("start_export response missing job_id")
        self.log.info(
            "Started export job_id=%s mode=%s max_page_size=%s",
            job_id,
            mode,
            page_size,
        )
        return job_id

    def get_export_status(self, job_id: str) -> dict:
        data = self._request("GET", f"exports/{job_id}")
        return {
            "status": data.get("status"),
            "download_url": data.get("download_url"),
            "error_message": data.get("error_message"),
        }

    def download_export_result(self, job_id: str, dest_path: str) -> int:
        import os

        response = self._request(
            "GET",
            f"exports/{job_id}/download",
            expect_json=False,
            stream=True,
        )
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        # сначала .tmp, потом rename — при падении не останется битый файл
        tmp_path = dest_path + ".tmp"
        written = 0
        with open(tmp_path, "wb") as fh:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    fh.write(chunk)
                    written += len(chunk)
        os.replace(tmp_path, dest_path)
        self.log.info("Downloaded job_id=%s to %s bytes=%s", job_id, dest_path, written)
        return written
