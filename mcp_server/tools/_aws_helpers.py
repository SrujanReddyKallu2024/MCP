"""
Shared AWS helpers used by multiple tool modules.

Avoids duplicating boto3 client creation and formatting code
across emr_tools, s3_tools, etc.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import boto3

from mcp_server.config import AWS_REGION, get_aws_profile


# ── Shared boto3 S3 client (fresh per call) ─────────────────────────────────


def get_s3_client(env: str | None = None):
    """Return a fresh boto3 S3 client for the given environment."""
    profile = get_aws_profile(env) or "default"
    session = boto3.Session(region_name=AWS_REGION, profile_name=profile)
    return session.client("s3")


# ── Shared formatting helpers ───────────────────────────────────────────────

def fmt_duration(start: Any, end: Any) -> str:
    """Human-readable duration between two timestamps (ISO strings or datetime objects)."""
    if not start:
        return "—"
    try:
        if isinstance(start, str):
            s = datetime.fromisoformat(start.replace("Z", "+00:00"))
        else:
            s = start if start.tzinfo else start.replace(tzinfo=timezone.utc)

        if end:
            if isinstance(end, str):
                e = datetime.fromisoformat(end.replace("Z", "+00:00"))
            else:
                e = end if end.tzinfo else end.replace(tzinfo=timezone.utc)
        else:
            e = datetime.now(timezone.utc)

        delta = e - s
        mins, secs = divmod(int(delta.total_seconds()), 60)
        hrs, mins = divmod(mins, 60)
        if hrs:
            return f"{hrs}h {mins}m {secs}s"
        if mins:
            return f"{mins}m {secs}s"
        return f"{secs}s"
    except Exception:
        return "—"


def fmt_size(size_bytes: int | float) -> str:
    """Format bytes to human-readable size."""
    for unit in ("B", "KB", "MB", "GB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"
