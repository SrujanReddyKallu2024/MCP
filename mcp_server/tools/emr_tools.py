"""
EMR Serverless tools.

Uses boto3 to list EMR Serverless applications, job runs, and read
Spark driver logs from S3.  Includes S3 log navigation for browsing
the log directory structure.

S3 log path structure (actual):
  s3://{bucket}/{prefix}/{process_name}/applications/{app_id}/jobs/{job_id}/SPARK_DRIVER/stdout.gz

Tools:
  • list_emr_applications  — list all EMR Serverless apps
  • list_job_runs          — job runs for an app, with optional state filter
  • get_job_run_details    — deep details for one job run
  • read_spark_driver_log  — read stdout/stderr from S3 SPARK_DRIVER path
  • browse_s3_logs         — navigate the S3 log directory structure
  • cancel_job_run         — cancel a running or pending job
  • read_s3_file           — read any file from S3 by URI
  • get_emr_cost_summary   — resource usage and cost summary across jobs
"""

from __future__ import annotations

import gzip
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.config import Config

from mcp_server.config import AWS_REGION, get_aws_profile, get_emr_log_bucket, EMR_LOG_PREFIX, validate_env, aws_env_header
from mcp_server.tools._aws_helpers import get_s3_client, clear_s3_client, fmt_duration, fmt_size, CREDENTIAL_ERROR_CODES


# ── Boto3 config with timeouts (prevents infinite hangs) ────────────────────

_BOTO_CONFIG = Config(
    connect_timeout=10,
    read_timeout=30,
    retries={"max_attempts": 2, "mode": "standard"},
)


# ── Boto3 client creation (cached per env, auto-retry on expired creds) ─────

_emr_cache: dict[str, object] = {}


def _get_emr(env: str | None = None):
    """Return a cached boto3 EMR Serverless client for the given environment."""
    key = (env or "default").lower()
    if key in _emr_cache:
        return _emr_cache[key]
    profile = get_aws_profile(env) or "default"
    session = boto3.Session(region_name=AWS_REGION, profile_name=profile)
    client = session.client("emr-serverless", config=_BOTO_CONFIG)
    _emr_cache[key] = client
    return client


def _clear_emr(env: str | None = None):
    """Clear cached EMR client for an env (call after credential refresh)."""
    key = (env or "default").lower()
    _emr_cache.pop(key, None)


# Use shared S3 client from _aws_helpers
_get_s3 = get_s3_client
_clear_s3 = clear_s3_client


# ── Formatting helpers ───────────────────────────────────────────────────────

_STATE_EMOJI = {
    "submitted":  "📤",
    "pending":    "🟡",
    "scheduled":  "🟡",
    "running":    "⏳",
    "success":    "✅",
    "failed":     "❌",
    "cancelling": "⛔",
    "cancelled":  "⛔",
    "created":    "🆕",
    "started":    "▶",
    "stopped":    "⏹",
    "terminated": "🛑",
}


def _emoji(state: str | None) -> str:
    return _STATE_EMOJI.get((state or "").lower(), "❓")


_fmt_duration = fmt_duration


def _error_code(exc: Exception) -> str:
    """Extract AWS error code from a botocore exception, or empty string."""
    resp = getattr(exc, 'response', None)
    if isinstance(resp, dict):
        return resp.get('Error', {}).get('Code', '')
    return ''


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split 's3://bucket/key' into (bucket, key)."""
    path = uri.replace("s3://", "")
    bucket, _, key = path.partition("/")
    return bucket, key


def _read_s3_object(bucket: str, key: str, env: str | None = None, _retried: bool = False) -> str | None:
    """Read an S3 object, auto-decompress gzip, return text or None.
    Auto-retries once on expired credentials (clears cached client)."""
    s3 = _get_s3(env)
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()
        # Auto-decompress gzip
        if key.endswith(".gz"):
            try:
                raw = gzip.decompress(raw)
            except Exception:
                pass
        return raw.decode("utf-8", errors="replace")
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as exc:
        # On credential errors, clear cache and retry once
        if not _retried and _error_code(exc) in CREDENTIAL_ERROR_CODES:
            _clear_s3(env)
            return _read_s3_object(bucket, key, env=env, _retried=True)
        return None


_fmt_size = fmt_size


# ═══════════════════════════════════════════════════════════════════════════════
# TOOLS
# ═══════════════════════════════════════════════════════════════════════════════


def list_emr_applications(states: str | None = None, env: str | None = None) -> str:
    """
    List all EMR Serverless applications.

    Note: DAGs create temporary EMR apps that are deleted after each run.
    If an app is not found here, it was already cleaned up — but job run
    details and S3 logs are still available via get_job_run_details and
    read_spark_driver_log using the application_id from the Airflow task log.

    Args:
        states: Optional comma-separated state filter (e.g. 'STARTED,CREATED').
        env: Target environment — 'dev', 'uat', 'test', or 'prod'.
             IMPORTANT: Do NOT guess or default. Ask the user which environment if not specified.

    Returns a formatted list of applications with IDs, types and states.
    """
    err = validate_env(env)
    if err:
        return err

    client = _get_emr(env)
    kwargs: dict[str, Any] = {"maxResults": 50}
    if states:
        kwargs["states"] = [s.strip().upper() for s in states.split(",")]

    try:
        resp = client.list_applications(**kwargs)
    except Exception as exc:
        return f"❌ Failed to list EMR Serverless apps: {exc}"

    apps = resp.get("applications", [])
    if not apps:
        return aws_env_header(env) + "No EMR Serverless applications found."

    lines = [aws_env_header(env), f"🖥️ **{len(apps)} EMR Serverless Application(s)**\n"]
    for app in apps:
        emoji = _emoji(app.get("state", ""))
        lines.append(f"{emoji} **{app.get('name', '?')}**")
        lines.append(f"   ID    : {app.get('id', '?')}")
        lines.append(f"   Type  : {app.get('type', '?')}")
        lines.append(f"   State : {app.get('state', '?')}")
        arn = app.get("arn", "")
        if arn:
            lines.append(f"   ARN   : {arn}")
        lines.append("")

    return "\n".join(lines)


def list_job_runs(
    application_id: str,
    max_results: int = 30,
    states: str | None = None,
    created_after: str | None = None,
    env: str | None = None,
) -> str:
    """
    List job runs for an EMR Serverless application.

    Args:
        application_id: The EMR Serverless application ID.
        max_results: Max runs to return (default 30).
        states: Optional comma-separated state filter (e.g. 'SUCCESS,FAILED').
        created_after: Optional ISO date — only runs after this date (e.g. '2026-02-16').
        env: Target environment — 'dev', 'uat', 'test', or 'prod'.
             IMPORTANT: Do NOT guess or default. Ask the user which environment if not specified.

    Returns a list of job runs with status, timing and duration.
    """
    err = validate_env(env)
    if err:
        return err

    client = _get_emr(env)
    kwargs: dict[str, Any] = {
        "applicationId": application_id,
        "maxResults": max_results,
    }
    if states:
        kwargs["states"] = [s.strip().upper() for s in states.split(",")]
    if created_after:
        try:
            kwargs["createdAtAfter"] = datetime.fromisoformat(created_after).replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    try:
        resp = client.list_job_runs(**kwargs)
    except Exception as exc:
        return f"❌ Failed to list job runs: {exc}"

    runs = resp.get("jobRuns", [])
    if not runs:
        return aws_env_header(env) + f"No job runs found for application `{application_id}`."

    lines = [aws_env_header(env), f"🚀 **{len(runs)} Job Run(s) for `{application_id}`**\n"]
    for run in runs:
        state = run.get("state", "?")
        emoji = _emoji(state)
        lines.append(f"{emoji} **{run.get('name', run.get('id', '?'))}**")
        lines.append(f"   Job ID   : {run.get('id', '?')}")
        lines.append(f"   State    : {state}")
        lines.append(f"   Created  : {run.get('createdAt', '—')}")
        lines.append(f"   Duration : {_fmt_duration(run.get('createdAt'), run.get('updatedAt'))}")
        lines.append("")

    return "\n".join(lines)


def get_job_run_details(application_id: str, job_run_id: str, env: str | None = None) -> str:
    """
    Get detailed information about a specific EMR Serverless job run.

    Shows the Spark submit config (entry point script, arguments), resource
    usage (vCPU hours, memory), and S3 log locations. Includes ready-to-use
    hints for read_spark_driver_log and browse_s3_logs.

    Args:
        application_id: The EMR Serverless application ID (from Airflow 'initialise' task log).
        job_run_id: The job run ID (from Airflow processing task log).
        env: Target environment — 'dev', 'uat', 'test', or 'prod'.
             IMPORTANT: Do NOT guess or default. Ask the user which environment if not specified.

    Returns comprehensive details: state, config, resource usage, S3 log paths.
    """
    err = validate_env(env)
    if err:
        return err

    client = _get_emr(env)

    try:
        resp = client.get_job_run(applicationId=application_id, jobRunId=job_run_id)
    except Exception as exc:
        return f"❌ Failed to get job run details: {exc}"

    run = resp.get("jobRun", {})
    state = run.get("state", "?")
    emoji = _emoji(state)

    lines = [
        aws_env_header(env),
        f"{emoji} **Job Run: {run.get('name', job_run_id)}**",
        f"   Job ID        : {run.get('jobRunId', '?')}",
        f"   Application   : {run.get('applicationId', '?')}",
        f"   State         : {state}",
        f"   State Details : {run.get('stateDetails', '—')}",
        f"   Created       : {run.get('createdAt', '—')}",
        f"   Updated       : {run.get('updatedAt', '—')}",
        f"   Duration      : {_fmt_duration(run.get('createdAt'), run.get('updatedAt'))}",
        f"   Release Label : {run.get('releaseLabel', '—')}",
    ]

    # Job driver info
    driver = run.get("jobDriver", {})
    spark = driver.get("sparkSubmit", {})
    if spark:
        lines.append("")
        lines.append("**Spark Submit Config:**")
        lines.append(f"   Entry Point     : {spark.get('entryPoint', '—')}")
        args = spark.get("entryPointArguments", [])
        if args:
            lines.append(f"   Arguments       : {' '.join(args[:10])}{'...' if len(args) > 10 else ''}")
        props = spark.get("sparkSubmitParameters", "")
        if props:
            lines.append(f"   Submit Params   : {props[:500]}{'...' if len(str(props)) > 500 else ''}")

    # Monitoring / log paths
    mon = run.get("monitoringConfiguration", {})
    s3_mon = mon.get("s3MonitoringConfiguration", {})
    if s3_mon:
        log_uri = s3_mon.get("logUri", "")
        lines.append("")
        lines.append("**S3 Log Location:**")
        lines.append(f"   Log URI : {log_uri}")
        lines.append(f"   💡 Use `read_spark_driver_log(application_id='{application_id}', job_run_id='{job_run_id}')` to read logs")
        lines.append(f"   💡 Use `browse_s3_logs(prefix='{log_uri.replace('s3://' + get_emr_log_bucket(env) + '/', '')}')` to browse")

    # Resource utilization
    ru = run.get("totalResourceUtilization", {})
    if ru:
        lines.append("")
        lines.append("**Resource Usage:**")
        lines.append(f"   vCPU Hours    : {ru.get('vCPUHour', 0):.2f}")
        lines.append(f"   Memory GB·Hrs : {ru.get('memoryGBHour', 0):.2f}")
        lines.append(f"   Storage GB·Hrs: {ru.get('storageGBHour', 0):.2f}")

    # Error details if failed
    if state.upper() in ("FAILED", "CANCELLED"):
        details = run.get("stateDetails", "")
        if details:
            lines.append("")
            lines.append(f"❌ **Failure Details:** {details}")

    return "\n".join(lines)


def read_spark_driver_log(
    application_id: str,
    job_run_id: str,
    log_type: str = "stdout",
    s3_log_uri: str | None = None,
    process_name: str | None = None,
    tail_lines: int = 300,
    search_text: str | None = None,
    bucket: str | None = None,
    read_both: bool = False,
    env: str | None = None,
) -> str:
    """
    Read the Spark driver log from S3 for an EMR Serverless job run.

    DEFAULT: Reads stdout.gz — this is the PRIMARY log containing Python print
    statements, row counts, file paths, and application errors. This is what
    you want 90% of the time.

    Use log_type='stderr' only when you need Spark framework logs (executor
    allocation, memory warnings, shuffle errors).

    Use read_both=True to get BOTH logs in one call (stdout first, then
    stderr filtered to ERROR lines only).

    How to find application_id and job_run_id:
      - application_id: from the 'initialise' Airflow task log → 'EMR serverless application created: 00gXXX'
      - job_run_id: from the processing Airflow task log → 'EMR serverless job started: 00gXXX'
      - Or use list_emr_applications() then list_job_runs()

    Args:
        application_id: The EMR Serverless application ID (e.g. '00g16i3marao0c0t').
        job_run_id: The job run ID (e.g. '00g16i5g2pm56o0v').
        log_type: 'stdout' (default, Python app output) or 'stderr' (Spark framework logs).
        s3_log_uri: Optional full S3 URI to read directly (e.g. 's3://bucket/path/stdout.gz').
        process_name: Optional folder name under spark-logs/ (e.g. 'stackadapt_main'). Speeds up log discovery.
        tail_lines: Number of lines from the end (default 300). Use -1 for all lines.
        search_text: Optional text to filter log lines (e.g. 'ERROR', 'Exception').
        bucket: S3 bucket override (default from config).
        read_both: If True, read BOTH stdout and stderr in one call. stdout shown first, stderr filtered to ERROR lines.

    Returns the log content, optionally filtered and tailed.
    """
    err = validate_env(env)
    if err:
        return err

    # If read_both, get stdout first then stderr (errors only)
    if read_both:
        stdout_result = read_spark_driver_log(
            application_id=application_id,
            job_run_id=job_run_id,
            log_type="stdout",
            s3_log_uri=s3_log_uri,
            process_name=process_name,
            tail_lines=tail_lines,
            search_text=search_text,
            bucket=bucket,
            env=env,
        )
        stderr_result = read_spark_driver_log(
            application_id=application_id,
            job_run_id=job_run_id,
            log_type="stderr",
            s3_log_uri=s3_log_uri.replace("stdout", "stderr") if s3_log_uri and "stdout" in s3_log_uri else s3_log_uri,
            process_name=process_name,
            tail_lines=min(tail_lines, 100),
            search_text="ERROR",
            bucket=bucket,
            env=env,
        )
        return (
            "═══ STDOUT (Python App Output — primary log) ═══\n"
            + stdout_result
            + "\n\n═══ STDERR (Spark Framework — errors only) ═══\n"
            + stderr_result
        )

    log_bucket = bucket or get_emr_log_bucket(env)

    # ── Path 1: Full S3 URI given → read directly (fastest, ~1s) ──
    if s3_log_uri:
        b, k = _parse_s3_uri(s3_log_uri)
        content = _read_s3_object(b, k, env=env)
        if content is None:
            return f"❌ Could not read: {s3_log_uri}"
        return _format_log_output(content, f"s3://{b}/{k}", log_type, tail_lines, search_text)

    # ── Path 2: process_name given → build direct S3 path (~1-3s) ──
    if process_name:
        prefix = EMR_LOG_PREFIX.strip("/")
        for suffix in [f"{log_type}.gz", log_type]:
            key = f"{prefix}/{process_name}/applications/{application_id}/jobs/{job_run_id}/SPARK_DRIVER/{suffix}"
            content = _read_s3_object(log_bucket, key, env=env)
            if content is not None:
                return _format_log_output(content, f"s3://{log_bucket}/{key}", log_type, tail_lines, search_text)

    # ── Path 3: S3 prefix scan — list process folders, try GetObject (~2-5s) ──
    # Lists process_name folders under spark-logs/ (1 cheap call), then tries
    # GetObject for each folder. NoSuchKey returns instantly (no timeout penalty),
    # so this is fast even with ~20-50 folders.
    s3 = _get_s3(env)
    scan_prefix = f"{EMR_LOG_PREFIX.strip('/')}/"
    try:
        resp = s3.list_objects_v2(
            Bucket=log_bucket, Prefix=scan_prefix, Delimiter="/", MaxKeys=200,
        )
        folders = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        for folder in folders:
            for suffix in [f"{log_type}.gz", log_type]:
                key = f"{folder}applications/{application_id}/jobs/{job_run_id}/SPARK_DRIVER/{suffix}"
                content = _read_s3_object(log_bucket, key, env=env)
                if content is not None:
                    return _format_log_output(content, f"s3://{log_bucket}/{key}", log_type, tail_lines, search_text)
    except Exception:
        pass  # Fall through to error guidance

    # ── Nothing worked — clear error with guidance ──
    error_lines: list[str] = []
    error_lines.append(f"❌ Could not find Spark driver `{log_type}` log.")
    error_lines.append("")
    error_lines.append("The log file should be at a path like:")
    error_lines.append(f"   `s3://{log_bucket}/spark-logs/{{process_name}}/applications/{application_id}/jobs/{job_run_id}/SPARK_DRIVER/{log_type}.gz`")
    error_lines.append("")
    error_lines.append("**How to fix:**")
    error_lines.append("1. Read the Airflow task log for this job — it contains the S3 log path")
    error_lines.append("2. Look for a line like: `spark-logs/<process_name>/applications/...`")
    error_lines.append(f"3. Then call: `read_spark_driver_log(application_id='{application_id}', job_run_id='{job_run_id}', s3_log_uri='s3://bucket/full/path/{log_type}.gz')`")
    error_lines.append(f"   Or: `read_spark_driver_log(application_id='{application_id}', job_run_id='{job_run_id}', process_name='<name from log>')`")
    return "\n".join(error_lines)


def _format_log_output(
    content: str,
    source: str,
    log_type: str,
    tail_lines: int,
    search_text: str | None,
) -> str:
    """Format log content with optional search and tail."""
    lines = content.splitlines()

    if search_text:
        search_lower = search_text.lower()
        lines = [l for l in lines if search_lower in l.lower()]
        header = f"📄 **Spark Driver {log_type.upper()}** — filtered for '{search_text}' ({len(lines)} matches)\n"
    else:
        total = len(lines)
        if tail_lines > 0 and total > tail_lines:
            lines = lines[-tail_lines:]
            header = f"📄 **Spark Driver {log_type.upper()}** (last {tail_lines} of {total} lines)\n"
        else:
            header = f"📄 **Spark Driver {log_type.upper()}** ({len(lines)} lines)\n"

    header += f"   Source: {source}\n"
    return header + "\n".join(lines)


def browse_s3_logs(
    prefix: str | None = None,
    bucket: str | None = None,
    max_items: int = 100,
    env: str | None = None,
) -> str:
    """
    Browse the S3 log directory structure. Navigate into folders to find logs.

    Args:
        prefix: S3 prefix/path to browse (default: EMR_LOG_PREFIX from config, e.g. 'spark-logs/').
                Use the output to navigate deeper, e.g. 'spark-logs/ttdgeo_metadata_SE/'.
        bucket: S3 bucket (default from config).
        max_items: Max items to show (default 50).
        env: Target environment — 'dev', 'uat', 'test', or 'prod'.
             IMPORTANT: Do NOT guess or default. Ask the user which environment if not specified.

    Returns a directory listing of the S3 prefix showing folders and files.
    """
    err = validate_env(env)
    if err:
        return err

    s3 = _get_s3(env)
    log_bucket = bucket or get_emr_log_bucket(env)
    log_prefix = prefix or (EMR_LOG_PREFIX.strip("/") + "/")

    # Ensure trailing slash for directory listing
    if log_prefix and not log_prefix.endswith("/"):
        log_prefix += "/"

    try:
        resp = s3.list_objects_v2(
            Bucket=log_bucket,
            Prefix=log_prefix,
            Delimiter="/",
            MaxKeys=max_items,
        )
    except Exception as exc:
        return f"❌ Failed to list S3: {exc}"

    folders = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
    files = resp.get("Contents", [])

    if not folders and not files:
        return aws_env_header(env) + f"📂 Empty: `s3://{log_bucket}/{log_prefix}` — no files or folders found."

    lines = [aws_env_header(env), f"📂 **Browsing: `s3://{log_bucket}/{log_prefix}`**\n"]

    # Show folders
    if folders:
        lines.append(f"**Folders ({len(folders)}):**")
        for folder in sorted(folders):
            # Show just the folder name (last segment)
            name = folder[len(log_prefix):].rstrip("/")
            lines.append(f"  📁 {name}/")
            lines.append(f"     → `browse_s3_logs(prefix='{folder}')`")
        lines.append("")

    # Show files
    if files:
        # Filter out the prefix itself (directory marker)
        real_files = [f for f in files if f["Key"] != log_prefix]
        if real_files:
            lines.append(f"**Files ({len(real_files)}):**")
            for f in sorted(real_files, key=lambda x: x["Key"]):
                name = f["Key"][len(log_prefix):]
                size = _fmt_size(f.get("Size", 0))
                modified = str(f.get("LastModified", ""))[:19]
                lines.append(f"  📄 {name}  ({size}, {modified})")
                # For log files, hint at how to read them
                if "SPARK_DRIVER" in f["Key"] or "sparkdriver" in f["Key"].lower():
                    lines.append(f"     → `read_spark_driver_log(s3_log_uri='s3://{log_bucket}/{f['Key']}')`")

    if resp.get("IsTruncated"):
        lines.append(f"\n⚠️ More items exist — showing first {max_items}. Use a more specific prefix to see deeper.")

    return "\n".join(lines)


def cancel_job_run(application_id: str, job_run_id: str, env: str | None = None) -> str:
    """
    Cancel a running or pending EMR Serverless job run.

    Use this when a Spark job is stuck, taking too long, or was started
    with incorrect parameters. The cancellation is asynchronous — the job
    will transition to CANCELLING and then CANCELLED state.

    Args:
        application_id: The EMR Serverless application ID.
        job_run_id: The job run ID to cancel.
        env: Target environment — 'dev', 'uat', 'test', or 'prod'.
             IMPORTANT: Do NOT guess or default. Ask the user which environment if not specified.

    Returns confirmation of the cancellation request.
    """
    err = validate_env(env)
    if err:
        return err

    client = _get_emr(env)

    try:
        client.cancel_job_run(applicationId=application_id, jobRunId=job_run_id)
    except Exception as exc:
        return f"❌ Failed to cancel job run `{job_run_id}`: {exc}"

    return (
        f"⛔ **Cancellation requested** for job `{job_run_id}` "
        f"on application `{application_id}`.\n"
        f"   The job will transition to CANCELLING → CANCELLED.\n"
        f"   💡 Use `get_job_run_details(application_id='{application_id}', "
        f"job_run_id='{job_run_id}')` to check the current state."
    )


def read_s3_file(
    s3_uri: str,
    tail_lines: int = 100,
    search_text: str | None = None,
    env: str | None = None,
) -> str:
    """
    Read any file from S3 by its full URI.

    Use this for reading input data files, output files, configuration files,
    or any S3 object — not just Spark logs. Auto-decompresses .gz files.

    Args:
        s3_uri: Full S3 URI (e.g. 's3://bucket-name/path/to/file.csv').
        tail_lines: Number of lines from the end to return (default 100). Set to -1 for all.
        search_text: Optional text to filter matching lines.
        env: Target environment — 'dev', 'uat', 'test', or 'prod'.
             IMPORTANT: Do NOT guess or default. Ask the user which environment if not specified.

    Returns the file contents, optionally filtered and tailed.
    """
    err = validate_env(env)
    if err:
        return err

    if not s3_uri.startswith("s3://"):
        return "❌ Invalid S3 URI: '{s3_uri}'. Must start with 's3://' (e.g. 's3://bucket-name/path/to/file.csv')."

    bucket, key = _parse_s3_uri(s3_uri)
    if not key:
        return f"❌ No key/path specified in URI: '{s3_uri}'."

    content = _read_s3_object(bucket, key, env=env)
    if content is None:
        return f"❌ Could not read `{s3_uri}`. Check that the file exists and you have access."

    lines_all = content.splitlines()
    total = len(lines_all)

    # Filter by search text
    if search_text:
        search_lower = search_text.lower()
        lines_all = [l for l in lines_all if search_lower in l.lower()]
        header = f"📄 **S3 File** — filtered for '{search_text}' ({len(lines_all)} matches of {total} lines)\n"
    else:
        if tail_lines > 0 and total > tail_lines:
            lines_all = lines_all[-tail_lines:]
            header = f"📄 **S3 File** (last {tail_lines} of {total} lines)\n"
        else:
            header = f"📄 **S3 File** ({total} lines)\n"

    header += f"   Source: {s3_uri}\n"
    return header + "\n".join(lines_all)


def get_emr_cost_summary(
    application_id: str | None = None,
    days: int = 7,
    env: str | None = None,
) -> str:
    """
    Get a summary of EMR Serverless resource usage and estimated costs.

    Aggregates vCPU hours, memory GB-hours, and storage GB-hours across
    recent job runs. Useful for understanding compute costs.

    Args:
        application_id: Optional — filter to one application. If omitted, scans all.
        days: Number of days to look back (default 7).
        env: Target environment — 'dev', 'uat', 'test', or 'prod'.
             IMPORTANT: Do NOT guess or default. Ask the user which environment if not specified.

    Returns a cost summary with per-job and total resource usage.
    """
    err = validate_env(env)
    if err:
        return err

    client = _get_emr(env)
    from datetime import timedelta

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    # Determine which apps to scan
    if application_id:
        app_ids = [application_id]
    else:
        try:
            resp = client.list_applications(maxResults=50)
            app_ids = [a["id"] for a in resp.get("applications", [])]
        except Exception as exc:
            return f"❌ Failed to list applications: {exc}"

    if not app_ids:
        return "No EMR Serverless applications found."

    total_vcpu = 0.0
    total_mem = 0.0
    total_storage = 0.0
    job_count = 0
    failed_count = 0
    per_app: list[dict] = []

    for aid in app_ids:
        try:
            resp = client.list_job_runs(
                applicationId=aid,
                maxResults=50,
                createdAtAfter=cutoff,
            )
            runs = resp.get("jobRuns", [])
        except Exception:
            continue

        app_vcpu = 0.0
        app_mem = 0.0
        app_storage = 0.0
        app_jobs = 0

        for run in runs:
            job_id = run.get("id", "?")
            try:
                detail_resp = client.get_job_run(applicationId=aid, jobRunId=job_id)
                job = detail_resp.get("jobRun", {})
                ru = job.get("totalResourceUtilization", {})
                vcpu = ru.get("vCPUHour", 0)
                mem = ru.get("memoryGBHour", 0)
                stor = ru.get("storageGBHour", 0)
                app_vcpu += vcpu
                app_mem += mem
                app_storage += stor
                app_jobs += 1
                if job.get("state", "").upper() == "FAILED":
                    failed_count += 1
            except Exception:
                continue

        if app_jobs > 0:
            per_app.append({
                "id": aid,
                "jobs": app_jobs,
                "vcpu": app_vcpu,
                "mem": app_mem,
                "storage": app_storage,
            })
            total_vcpu += app_vcpu
            total_mem += app_mem
            total_storage += app_storage
            job_count += app_jobs

    if job_count == 0:
        return f"No job runs found in the last {days} day(s)."

    lines = [
        aws_env_header(env),
        f"💰 **EMR Serverless Cost Summary — Last {days} Day(s)**",
        "",
        f"**Totals across {job_count} job(s), {len(per_app)} app(s):**",
        f"   vCPU Hours     : {total_vcpu:.2f}",
        f"   Memory GB·Hrs  : {total_mem:.2f}",
        f"   Storage GB·Hrs : {total_storage:.2f}",
        f"   Failed Jobs    : {failed_count}",
        "",
    ]

    if len(per_app) > 1:
        lines.append("**Per Application:**")
        lines.append(f"{'Application ID':<22} {'Jobs':>5} {'vCPU Hrs':>10} {'Mem GB·Hrs':>12} {'Stor GB·Hrs':>12}")
        lines.append("─" * 65)
        for a in sorted(per_app, key=lambda x: -x["vcpu"]):
            lines.append(
                f"{a['id']:<22} {a['jobs']:>5} {a['vcpu']:>10.2f} {a['mem']:>12.2f} {a['storage']:>12.2f}"
            )

    return "\n".join(lines)
