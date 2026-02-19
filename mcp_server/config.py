"""
Centralized configuration loader.

Reads environment variables from .env file and exposes typed config
that all tool modules use.  Supports multiple MWAA environments (dev/test/prod).
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ── Load .env from project root ──────────────────────────────────────────────
_project_root = Path(__file__).resolve().parent.parent
_env_path = _project_root / ".env"
load_dotenv(_env_path)


# ── AWS ──────────────────────────────────────────────────────────────────────

AWS_REGION: str = os.getenv("AWS_REGION", "eu-west-2")
AWS_PROFILE: str | None = os.getenv("AWS_PROFILE", "consumersync") or None

# ── MWAA — multiple environments ────────────────────────────────────────────
# Store as dict so tools can target dev / test / prod by name
# Hardcoded defaults so the server works out-of-the-box from the MCP Registry.

_MWAA_DEFAULTS: dict[str, str] = {
    "dev":  "eec-aws-uk-ms-dev-consumersyncenv-mwaa",
    "test": "eec-aws-uk-ms-tst-consumersyncenv-mwaa",
    "prod": "eec-aws-uk-ms-prod-consumersyncenv-mwaa",
}

MWAA_ENVIRONMENTS: dict[str, str] = {}
# Read MWAA_ENV_DEV, MWAA_ENV_TEST, MWAA_ENV_PROD (env vars override defaults)
for env_tag in ("DEV", "TEST", "PROD"):
    val = os.getenv(f"MWAA_ENV_{env_tag}", _MWAA_DEFAULTS.get(env_tag.lower(), ""))
    if val:
        MWAA_ENVIRONMENTS[env_tag.lower()] = val

# Fallback: single MWAA_ENV_NAME
_single = os.getenv("MWAA_ENV_NAME", "")
if _single and not MWAA_ENVIRONMENTS:
    MWAA_ENVIRONMENTS["default"] = _single

DEFAULT_MWAA_ENV: str = os.getenv("MWAA_DEFAULT_ENV", "dev")

# ── EMR Serverless ───────────────────────────────────────────────────────────

EMR_LOG_BUCKET: str = os.getenv("EMR_LOG_BUCKET", "eec-aws-uk-ms-consumersync-dev-logs-bucket")
EMR_LOG_PREFIX: str = os.getenv("EMR_LOG_PREFIX", "spark-logs")

# ── Confluence ───────────────────────────────────────────────────────────────

CONFLUENCE_BASE_URL: str = os.getenv("CONFLUENCE_BASE_URL", "https://pages.experian.local")
CONFLUENCE_PAT: str = os.getenv("CONFLUENCE_PAT", "")
CONFLUENCE_SPACE_KEY: str = os.getenv("CONFLUENCE_SPACE_KEY", "ACTIVATE")
CONFLUENCE_ROOT_PAGE_ID: str = os.getenv("CONFLUENCE_ROOT_PAGE_ID", "")  # Scope to page tree


# ── Validators ───────────────────────────────────────────────────────────────

def get_mwaa_env_name(env: str | None = None) -> str | None:
    """
    Resolve an MWAA environment name.
    Returns the env name string or None if not configured.
    """
    tag = (env or DEFAULT_MWAA_ENV).strip().lower()
    return MWAA_ENVIRONMENTS.get(tag)


def validate_mwaa(env: str | None = None) -> str:
    """Return error message if MWAA env is not configured, else empty string."""
    name = get_mwaa_env_name(env)
    if not name:
        configured = ", ".join(MWAA_ENVIRONMENTS.keys()) if MWAA_ENVIRONMENTS else "none"
        return (
            f"⚠️  MWAA environment '{env or DEFAULT_MWAA_ENV}' is not configured.\n"
            f"   Configured environments: {configured}\n"
            f"   Set MWAA_ENV_DEV / MWAA_ENV_TEST / MWAA_ENV_PROD in .env"
        )
    return ""


def validate_confluence() -> str:
    """Return error message or empty string if confluence is configured."""
    if not CONFLUENCE_PAT:
        return "⚠️  CONFLUENCE_PAT is not set in .env — please set it before using Confluence tools."
    return ""
