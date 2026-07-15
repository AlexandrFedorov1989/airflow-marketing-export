import csv
import io
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from models import ExportFormat, ExportMode, ExportRequest, ExportStatus


@dataclass
class JobRecord:
    job_id: str
    request: ExportRequest
    status: ExportStatus = ExportStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    poll_count: int = 0
    force_fail: bool = False
    events: List[dict] = field(default_factory=list)


class JobStore:
    # jobs в памяти; статус меняется при каждом опросе (как будто async выгрузка)

    def __init__(self) -> None:
        self._jobs: Dict[str, JobRecord] = {}

    def create_job(self, request: ExportRequest, force_fail: bool = False) -> JobRecord:
        job_id = str(uuid.uuid4())
        events = _generate_events(request)
        job = JobRecord(
            job_id=job_id,
            request=request,
            force_fail=force_fail,
            events=events,
        )
        self._jobs[job_id] = job
        return job

    def get_job(self, job_id: str) -> Optional[JobRecord]:
        return self._jobs.get(job_id)

    def advance_status(self, job: JobRecord) -> JobRecord:
        job.poll_count += 1
        if job.force_fail and job.poll_count >= 2:
            job.status = ExportStatus.FAILED
            return job

        if job.status == ExportStatus.PENDING:
            job.status = ExportStatus.RUNNING
        elif job.status == ExportStatus.RUNNING and job.poll_count >= 3:
            job.status = ExportStatus.COMPLETED
        return job

    def render_download(self, job: JobRecord) -> bytes:
        if job.request.format == ExportFormat.CSV:
            return _to_csv(job.events)
        return _to_jsonl(job.events)


def _generate_events(request: ExportRequest) -> List[dict]:
    base = [
        {
            "event_id": f"evt-{i:04d}",
            "user_id": f"user-{i % 5}",
            "campaign_id": "camp-demo",
            "event_type": "click" if i % 2 else "impression",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        for i in range(1, 11)
    ]
    if request.mode == ExportMode.INCREMENTAL and request.updated_after:
        return [e for e in base if e["updated_at"] >= request.updated_after]
    return base


def _to_jsonl(events: List[dict]) -> bytes:
    lines = [json.dumps(e, ensure_ascii=False) for e in events]
    return ("\n".join(lines) + "\n").encode("utf-8")


def _to_csv(events: List[dict]) -> bytes:
    if not events:
        return b""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=events[0].keys())
    writer.writeheader()
    writer.writerows(events)
    return buffer.getvalue().encode("utf-8")
