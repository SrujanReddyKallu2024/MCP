"""
Centralized configuration loader.

Reads environment variables (from .env for local dev, or injected by VS Code /
MCP registry for installed servers) and exposes typed config that all tool
modules use.

Supports four environments: dev, uat, test, prod.
Each environment can have its own AWS CLI profile, MWAA instance name,
and EMR log bucket — because they live in **different AWS accounts**.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ── Load .env from project root (local dev only — not used in MCP installs) ──
_project_root = Path(__file__).resolve().parent.parent
_env_path = _project_root / ".env"
load_dotenv(_env_path)


# ── AWS — shared ─────────────────────────────────────────────────────────────

AWS_REGION: str = os.getenv("AWS_REGION", "eu-west-2")

# Legacy single profile (kept for backward compat — old .env files)
AWS_PROFILE: str | None = os.getenv("AWS_PROFILE", "consumersync") or None

# Default environment when 'env' param is omitted
DEFAULT_ENV: str = os.getenv("DEFAULT_ENV", "dev")


# ── AWS — per-environment profiles ───────────────────────────────────────────
# Each env is a different AWS account → different gimme-aws-creds profile.
# Reads AWS_PROFILE_DEV, AWS_PROFILE_UAT, AWS_PROFILE_TEST, AWS_PROFILE_PROD.
# Falls back to single AWS_PROFILE if no per-env vars are set (backward compat).

_PROFILE_DEFAULTS: dict[str, str] = {
    "dev":  "consumersync-dev",
    "uat":  "consumersync-uat",
    "test": "consumersync-test",
    "prod": "consumersync-prod",
}

AWS_PROFILES: dict[str, str] = {}
for _env_tag in ("DEV", "UAT", "TEST", "PROD"):
    _val = os.getenv(f"AWS_PROFILE_{_env_tag}", "")
    AWS_PROFILES[_env_tag.lower()] = _val or _PROFILE_DEFAULTS[_env_tag.lower()]

# Legacy fallback: if no per-env vars set and single AWS_PROFILE exists, use it for all
_any_profile_set = any(os.getenv(f"AWS_PROFILE_{t}") for t in ("DEV", "UAT", "TEST", "PROD"))
if not _any_profile_set and AWS_PROFILE:
    for _k in AWS_PROFILES:
        AWS_PROFILES[_k] = AWS_PROFILE


# ── MWAA — multiple environments ─────────────────────────────────────────────
# Note: only dev has "consumersyncenv" in the name; uat/test/prod have "consumersync".

_MWAA_DEFAULTS: dict[str, str] = {
    "dev":  "eec-aws-uk-ms-dev-consumersyncenv-mwaa",
    "uat":  "eec-aws-uk-ms-uat-consumersync-mwaa",
    "test": "eec-aws-uk-ms-tst-consumersync-mwaa",
    "prod": "eec-aws-uk-ms-prod-consumersync-mwaa",
}

MWAA_ENVIRONMENTS: dict[str, str] = {}
for _env_tag in ("DEV", "UAT", "TEST", "PROD"):
    _val = os.getenv(f"MWAA_ENV_{_env_tag}", _MWAA_DEFAULTS.get(_env_tag.lower(), ""))
    if _val:
        MWAA_ENVIRONMENTS[_env_tag.lower()] = _val

# Fallback: single MWAA_ENV_NAME (legacy)
_single = os.getenv("MWAA_ENV_NAME", "")
if _single and not MWAA_ENVIRONMENTS:
    MWAA_ENVIRONMENTS["default"] = _single

DEFAULT_MWAA_ENV: str = os.getenv("MWAA_DEFAULT_ENV", DEFAULT_ENV)


# ── EMR Serverless — per-environment log buckets ─────────────────────────────

_EMR_BUCKET_DEFAULTS: dict[str, str] = {
    "dev":  "eec-aws-uk-ms-consumersync-dev-logs-bucket",
    "uat":  "eec-aws-uk-ms-consumersync-uat-logs-bucket",
    "test": "eec-aws-uk-ms-consumersync-tst-logs-bucket",
    "prod": "eec-aws-uk-ms-consumersync-prod-logs-bucket",
}

EMR_LOG_BUCKETS: dict[str, str] = {}
for _env_tag in ("DEV", "UAT", "TEST", "PROD"):
    _val = os.getenv(
        f"EMR_LOG_BUCKET_{_env_tag}",
        _EMR_BUCKET_DEFAULTS.get(_env_tag.lower(), ""),
    )
    if _val:
        EMR_LOG_BUCKETS[_env_tag.lower()] = _val

# Legacy fallback: if no per-env vars set, use single EMR_LOG_BUCKET for all
_any_emr_set = any(os.getenv(f"EMR_LOG_BUCKET_{t}") for t in ("DEV", "UAT", "TEST", "PROD"))
if not _any_emr_set:
    _legacy_bucket = os.getenv("EMR_LOG_BUCKET", _EMR_BUCKET_DEFAULTS["dev"])
    for _k in EMR_LOG_BUCKETS:
        EMR_LOG_BUCKETS[_k] = _legacy_bucket

# Keep old name as alias for backward compat (direct imports in other modules)
EMR_LOG_BUCKET: str = EMR_LOG_BUCKETS.get(DEFAULT_ENV, _EMR_BUCKET_DEFAULTS["dev"])
EMR_LOG_PREFIX: str = os.getenv("EMR_LOG_PREFIX", "spark-logs")


# ── Confluence ────────────────────────────────────────────────────────────────

CONFLUENCE_BASE_URL: str = os.getenv("CONFLUENCE_BASE_URL", "https://pages.experian.local")
CONFLUENCE_PAT: str = os.getenv("CONFLUENCE_PAT", "")
CONFLUENCE_SPACE_KEY: str = os.getenv("CONFLUENCE_SPACE_KEY", "ACTIVATE")
CONFLUENCE_ROOT_PAGE_ID: str = os.getenv("CONFLUENCE_ROOT_PAGE_ID", "")


# ── Azure DevOps (TFS) ───────────────────────────────────────────────────────

AZDO_BASE_URL: str = os.getenv("AZDO_BASE_URL", "https://ukfhpapcvt02.uk.experian.local/tfs/DefaultCollection")
AZDO_PAT: str = os.getenv("AZDO_PAT", "")
AZDO_PROJECT: str = os.getenv("AZDO_PROJECT", "Activate")
AZDO_TEAM: str = os.getenv("AZDO_TEAM", "Activate Team")


# ── Resolvers ─────────────────────────────────────────────────────────────────

def get_aws_profile(env: str | None = None) -> str | None:
    """Resolve the AWS CLI profile for a given environment."""
    tag = (env or DEFAULT_ENV).strip().lower()
    return AWS_PROFILES.get(tag) or AWS_PROFILE or None


def get_emr_log_bucket(env: str | None = None) -> str:
    """Resolve the EMR log bucket for a given environment."""
    tag = (env or DEFAULT_ENV).strip().lower()
    return EMR_LOG_BUCKETS.get(tag, EMR_LOG_BUCKET)


def get_mwaa_env_name(env: str | None = None) -> str | None:
    """Resolve an MWAA environment name."""
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
            f"   Set MWAA_ENV_DEV / MWAA_ENV_UAT / MWAA_ENV_TEST / MWAA_ENV_PROD"
        )
    return ""


VALID_ENVS = frozenset(AWS_PROFILES.keys())   # {"dev", "uat", "test", "prod"}


def validate_env(env: str | None) -> str:
    """Return error message if env is not a recognised environment, else empty string."""
    if env is None:
        return ""
    tag = env.strip().lower()
    if tag not in VALID_ENVS:
        return (
            f"⚠️  Unknown environment '{env}'.\n"
            f"   Valid environments: {', '.join(sorted(VALID_ENVS))}"
        )
    return ""


def aws_env_header(env: str | None = None) -> str:
    """Friendly header showing which AWS account/environment we're talking to."""
    tag = (env or DEFAULT_ENV).upper()
    profile = get_aws_profile(env) or "?"
    available = ", ".join(k.upper() for k in sorted(AWS_PROFILES.keys()))
    return f"🌍 Environment: **{tag}** (profile: `{profile}`)  |  Available: {available}\n"


def validate_confluence() -> str:
    """Return error message or empty string if confluence is configured."""
    if not CONFLUENCE_PAT:
        return "⚠️  CONFLUENCE_PAT is not set — please set it before using Confluence tools."
    return ""


def validate_azuredevops() -> str:
    """Return error message or empty string if Azure DevOps is configured."""
    if not AZDO_BASE_URL:
        return "⚠️  AZDO_BASE_URL not set — e.g. https://server/tfs/DefaultCollection"
    if not AZDO_PAT:
        return "⚠️  AZDO_PAT not set — generate a PAT in Azure DevOps → User Settings → Personal Access Tokens"
    if not AZDO_PROJECT:
        return "⚠️  AZDO_PROJECT not set — set to your project name"
    return ""
