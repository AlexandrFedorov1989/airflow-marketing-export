from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, Field


class ExportMode(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"


class ExportFormat(str, Enum):
    JSONL = "jsonl"
    CSV = "csv"


class ExportStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class ExportRequest(BaseModel):
    date_from: str = Field(..., description="Start date YYYY-MM-DD")
    date_to: str = Field(..., description="End date YYYY-MM-DD")
    format: ExportFormat = ExportFormat.JSONL
    mode: ExportMode = ExportMode.FULL
    updated_after: Optional[str] = Field(None, description="ISO timestamp for incremental")
    max_page_size: Optional[int] = Field(None, description="Optional page size hint from client")


class ExportStartResponse(BaseModel):
    job_id: str


class ExportStatusResponse(BaseModel):
    job_id: str
    status: ExportStatus
    download_url: Optional[str] = None
    error_message: Optional[str] = None


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"
