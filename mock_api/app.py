import asyncio
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import Response

from models import (
    ExportStartResponse,
    ExportStatus,
    ExportStatusResponse,
    ExportRequest,
    HealthResponse,
)
from storage import JobStore

# простой mock внешнего API: старт выгрузки → статус → скачать файл
app = FastAPI(title="Mock Marketing API", version="1.0.0")
store = JobStore()


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse()


@app.post("/api/v1/exports", response_model=ExportStartResponse, status_code=202)
async def start_export(
    request: Request,
    body: ExportRequest,
    simulate: Optional[str] = Query(None, description="429|500|timeout"),
) -> ExportStartResponse:
    # simulate нужен чтобы руками проверить ретраи/ошибки в Airflow
    await _maybe_simulate(simulate, stage="start")

    force_fail = simulate == "job_failed"
    job = store.create_job(body, force_fail=force_fail)
    return ExportStartResponse(job_id=job.job_id)


@app.get("/api/v1/exports/{job_id}", response_model=ExportStatusResponse)
async def get_export_status(
    job_id: str,
    simulate: Optional[str] = Query(None, description="429|500|timeout"),
) -> ExportStatusResponse:
    await _maybe_simulate(simulate, stage="status")

    job = store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    job = store.advance_status(job)
    download_url = None
    if job.status == ExportStatus.COMPLETED:
        download_url = f"/api/v1/exports/{job_id}/download"

    error_message = None
    if job.status == ExportStatus.FAILED:
        error_message = "Simulated export failure"

    return ExportStatusResponse(
        job_id=job.job_id,
        status=job.status,
        download_url=download_url,
        error_message=error_message,
    )


@app.get("/api/v1/exports/{job_id}/download")
async def download_export(
    job_id: str,
    simulate: Optional[str] = Query(None, description="429|500|timeout|empty"),
) -> Response:
    await _maybe_simulate(simulate, stage="download")

    job = store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    if job.status != ExportStatus.COMPLETED:
        raise HTTPException(status_code=409, detail="Export is not completed yet")

    content = store.render_download(job)
    if simulate == "empty":
        content = b""

    media_type = "application/x-ndjson" if job.request.format.value == "jsonl" else "text/csv"
    filename = f"export.{job.request.format.value}"
    return Response(
        content=content,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


async def _maybe_simulate(simulate: Optional[str], stage: str) -> None:
    if simulate == "timeout":
        await asyncio.sleep(120)
    if simulate == "429":
        raise HTTPException(status_code=429, detail=f"Rate limit on {stage}")
    if simulate == "500":
        raise HTTPException(status_code=500, detail=f"Server error on {stage}")
