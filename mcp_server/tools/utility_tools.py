"""
Utility tools — server health, environment info.

Provides a health-check tool so the AI can verify connectivity
to all configured services before attempting operations.

Tools:
  • server_health_check — test connectivity to MWAA, EMR, S3, Confluence, Azure DevOps
"""

from __future__ import annotations

import boto3
import requests
import urllib3

from mcp_server.config import (
    AWS_REGION,
    AWS_PROFILE,
    MWAA_ENVIRONMENTS,
    DEFAULT_MWAA_ENV,
    EMR_LOG_BUCKET,
    EMR_LOG_PREFIX,
    CONFLUENCE_BASE_URL,
    CONFLUENCE_PAT,
    CONFLUENCE_SPACE_KEY,
    AZDO_BASE_URL,
    AZDO_PAT,
    AZDO_PROJECT,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def server_health_check() -> str:
    """
    Check connectivity to all configured services.

    Tests AWS credentials, MWAA environments, EMR Serverless API,
    S3 log bucket access, Confluence PAT, and Azure DevOps connectivity.

    Use this FIRST to verify everything is connected before running
    other tools. Requires VPN and valid AWS credentials.

    Returns a status report for each service.
    """
    lines = ["🏥 **Ops-Tools Health Check**\n"]

    # ── AWS Credentials ──
    try:
        session = boto3.Session(region_name=AWS_REGION, profile_name=AWS_PROFILE)
        sts = session.client("sts")
        identity = sts.get_caller_identity()
        account = identity.get("Account", "?")
        arn = identity.get("Arn", "?")
        lines.append("✅ **AWS Credentials**")
        lines.append(f"   Account : {account}")
        lines.append(f"   ARN     : {arn}")
        lines.append(f"   Region  : {AWS_REGION}")
        lines.append(f"   Profile : {AWS_PROFILE or 'default'}")
    except Exception as exc:
        lines.append(f"❌ **AWS Credentials** — {exc}")
    lines.append("")

    # ── MWAA Environments ──
    if MWAA_ENVIRONMENTS:
        lines.append(f"✅ **MWAA Environments** ({len(MWAA_ENVIRONMENTS)} configured)")
        for tag, name in MWAA_ENVIRONMENTS.items():
            marker = " ← default" if tag == DEFAULT_MWAA_ENV else ""
            lines.append(f"   {tag.upper():6s} : {name}{marker}")
    else:
        lines.append("❌ **MWAA Environments** — none configured")
    lines.append("")

    # ── MWAA Connectivity (test the default env) ──
    if MWAA_ENVIRONMENTS:
        default_env = MWAA_ENVIRONMENTS.get(DEFAULT_MWAA_ENV)
        if default_env:
            try:
                session = boto3.Session(region_name=AWS_REGION, profile_name=AWS_PROFILE)
                mwaa = session.client("mwaa")
                token_resp = mwaa.create_web_login_token(Name=default_env)
                hostname = token_resp.get("WebServerHostname", "?")
                lines.append(f"✅ **MWAA API** — token obtained for `{default_env}`")
                lines.append(f"   Hostname: {hostname}")
            except Exception as exc:
                lines.append(f"⚠️  **MWAA API** — token request failed: {exc}")
        lines.append("")

    # ── EMR Serverless ──
    try:
        session = boto3.Session(region_name=AWS_REGION, profile_name=AWS_PROFILE)
        emr = session.client("emr-serverless")
        resp = emr.list_applications(maxResults=1)
        app_count = len(resp.get("applications", []))
        lines.append(f"✅ **EMR Serverless** — API accessible ({app_count} app(s) visible)")
    except Exception as exc:
        lines.append(f"❌ **EMR Serverless** — {exc}")
    lines.append("")

    # ── S3 Log Bucket ──
    try:
        session = boto3.Session(region_name=AWS_REGION, profile_name=AWS_PROFILE)
        s3 = session.client("s3")
        prefix = EMR_LOG_PREFIX.strip("/") + "/"
        resp = s3.list_objects_v2(Bucket=EMR_LOG_BUCKET, Prefix=prefix, MaxKeys=1)
        found = resp.get("KeyCount", 0)
        lines.append(f"✅ **S3 Log Bucket** — `{EMR_LOG_BUCKET}` accessible")
        lines.append(f"   Prefix  : {prefix} ({found} object(s) found)")
    except Exception as exc:
        lines.append(f"❌ **S3 Log Bucket** — `{EMR_LOG_BUCKET}`: {exc}")
    lines.append("")

    # ── Confluence ──
    if not CONFLUENCE_PAT:
        lines.append("❌ **Confluence** — PAT not configured")
    else:
        try:
            sess = requests.Session()
            sess.headers.update({
                "Authorization": f"Bearer {CONFLUENCE_PAT}",
                "Accept": "application/json",
            })
            sess.verify = False
            resp = sess.get(
                f"{CONFLUENCE_BASE_URL.rstrip('/')}/rest/api/space/{CONFLUENCE_SPACE_KEY}",
                timeout=15,
            )
            if resp.status_code == 200:
                space_data = resp.json()
                space_name = space_data.get("name", CONFLUENCE_SPACE_KEY)
                lines.append(f"✅ **Confluence** — connected to `{space_name}`")
                lines.append(f"   URL     : {CONFLUENCE_BASE_URL}")
                lines.append(f"   Space   : {CONFLUENCE_SPACE_KEY}")
            else:
                lines.append(f"⚠️  **Confluence** — HTTP {resp.status_code}")
        except Exception as exc:
            lines.append(f"❌ **Confluence** — {exc}")
    lines.append("")

    # ── Azure DevOps (TFS) ──
    if not AZDO_PAT:
        lines.append("❌ **Azure DevOps** — PAT not configured")
    elif not AZDO_BASE_URL:
        lines.append("❌ **Azure DevOps** — AZDO_BASE_URL not set")
    else:
        try:
            import base64
            token = base64.b64encode(f":{AZDO_PAT}".encode()).decode()
            sess = requests.Session()
            sess.headers.update({
                "Authorization": f"Basic {token}",
                "Accept": "application/json",
            })
            sess.verify = False
            resp = sess.get(
                f"{AZDO_BASE_URL.rstrip('/')}/_apis/projects/{AZDO_PROJECT}",
                params={"api-version": "7.1"},
                timeout=15,
            )
            if resp.status_code == 200:
                proj_data = resp.json()
                proj_name = proj_data.get("name", AZDO_PROJECT)
                proj_state = proj_data.get("state", "?")
                lines.append(f"✅ **Azure DevOps** — connected to `{proj_name}` ({proj_state})")
                lines.append(f"   URL     : {AZDO_BASE_URL}")
                lines.append(f"   Project : {AZDO_PROJECT}")
            else:
                lines.append(f"⚠️  **Azure DevOps** — HTTP {resp.status_code}")
        except Exception as exc:
            lines.append(f"❌ **Azure DevOps** — {exc}")
    lines.append("")

    # ── Summary ──
    full = "\n".join(lines)
    ok_count = full.count("✅")
    fail_count = full.count("❌")
    warn_count = full.count("⚠️")
    lines.append(f"**Summary:** {ok_count} OK, {warn_count} warnings, {fail_count} failures")

    return "\n".join(lines)
