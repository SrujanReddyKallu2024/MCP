"""
Shared AWS helpers used by multiple tool modules.

Avoids duplicating boto3 client creation and formatting code
across emr_tools, s3_tools, etc.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.config import Config

from mcp_server.config import AWS_REGION, get_aws_profile


# ── Boto3 config with timeouts (prevents infinite hangs) ────────────────────

_BOTO_CONFIG = Config(
    connect_timeout=10,
    read_timeout=30,
    retries={"max_attempts": 2, "mode": "standard"},
)


# ── Credential error codes that indicate expired/invalid SSO tokens ──────────

CREDENTIAL_ERROR_CODES = frozenset({
    "ExpiredToken", "ExpiredTokenException",
    "InvalidToken", "AccessDenied", "AccessDeniedException",
    "UnrecognizedClientException", "InvalidIdentityToken",
})


# ── Shared boto3 S3 client (cached per env, auto-retry on expired creds) ────

_s3_cache: dict[str, object] = {}


def get_s3_client(env: str | None = None):
    """Return a cached boto3 S3 client for the given environment."""
    key = (env or "default").lower()
    if key in _s3_cache:
        return _s3_cache[key]
    profile = get_aws_profile(env) or "default"
    session = boto3.Session(region_name=AWS_REGION, profile_name=profile)
    client = session.client("s3", config=_BOTO_CONFIG)
    _s3_cache[key] = client
    return client


def clear_s3_client(env: str | None = None):
    """Clear cached S3 client for an env (call after credential refresh)."""
    key = (env or "default").lower()
    _s3_cache.pop(key, None)


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
