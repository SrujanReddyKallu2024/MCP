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

import json
from datetime import datetime, timezone
from typing import Any

import boto3

from mcp_server.config import AWS_REGION, AWS_PROFILE, EMR_LOG_BUCKET, EMR_LOG_PREFIX


# ── Boto3 client singletons ──────────────────────────────────────────────────

_emr_client = None
_s3_client = None


def _get_emr():
    global _emr_client
    if _emr_client is None:
        session = boto3.Session(region_name=AWS_REGION, profile_name=AWS_PROFILE)
        _emr_client = session.client("emr-serverless")
    return _emr_client


def _get_s3():
    global _s3_client
    if _s3_client is None:
        session = boto3.Session(region_name=AWS_REGION, profile_name=AWS_PROFILE)
        _s3_client = session.client("s3")
    return _s3_client


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


def _fmt_duration(start: Any, end: Any) -> str:
    """Human-readable duration."""
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


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split 's3://bucket/key' into (bucket, key)."""
    path = uri.replace("s3://", "")
    bucket, _, key = path.partition("/")
    return bucket, key


def _read_s3_object(bucket: str, key: str) -> str | None:
    """Read an S3 object, auto-decompress gzip, return text or None."""
    s3 = _get_s3()
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
    except Exception:
        return None


def _fmt_size(size_bytes: int) -> str:
    """Format bytes to human-readable size."""
    for unit in ("B", "KB", "MB", "GB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


# ═══════════════════════════════════════════════════════════════════════════════
# TOOLS
# ═══════════════════════════════════════════════════════════════════════════════


def list_emr_applications(states: str | None = None) -> str:
    """
    List all EMR Serverless applications.

    Note: DAGs create temporary EMR apps that are deleted after each run.
    If an app is not found here, it was already cleaned up — but job run
    details and S3 logs are still available via get_job_run_details and
    read_spark_driver_log using the application_id from the Airflow task log.

    Args:
        states: Optional comma-separated state filter (e.g. 'STARTED,CREATED').

    Returns a formatted list of applications with IDs, types and states.
    """
    client = _get_emr()
    kwargs: dict[str, Any] = {"maxResults": 50}
    if states:
        kwargs["states"] = [s.strip().upper() for s in states.split(",")]

    try:
        resp = client.list_applications(**kwargs)
    except Exception as exc:
        return f"❌ Failed to list EMR Serverless apps: {exc}"

    apps = resp.get("applications", [])
    if not apps:
        return "No EMR Serverless applications found."

    lines = [f"🖥️ **{len(apps)} EMR Serverless Application(s)**\n"]
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
) -> str:
    """
    List job runs for an EMR Serverless application.

    Args:
        application_id: The EMR Serverless application ID.
        max_results: Max runs to return (default 30).
        states: Optional comma-separated state filter (e.g. 'SUCCESS,FAILED').
        created_after: Optional ISO date — only runs after this date (e.g. '2026-02-16').

    Returns a list of job runs with status, timing and duration.
    """
    client = _get_emr()
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
        return f"No job runs found for application `{application_id}`."

    lines = [f"🚀 **{len(runs)} Job Run(s) for `{application_id}`**\n"]
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


def get_job_run_details(application_id: str, job_run_id: str) -> str:
    """
    Get detailed information about a specific EMR Serverless job run.

    Shows the Spark submit config (entry point script, arguments), resource
    usage (vCPU hours, memory), and S3 log locations. Includes ready-to-use
    hints for read_spark_driver_log and browse_s3_logs.

    Args:
        application_id: The EMR Serverless application ID (from Airflow 'initialise' task log).
        job_run_id: The job run ID (from Airflow processing task log).

    Returns comprehensive details: state, config, resource usage, S3 log paths.
    """
    client = _get_emr()

    try:
        resp = client.get_job_run(applicationId=application_id, jobRunId=job_run_id)
    except Exception as exc:
        return f"❌ Failed to get job run details: {exc}"

    run = resp.get("jobRun", {})
    state = run.get("state", "?")
    emoji = _emoji(state)

    lines = [
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
        lines.append(f"   💡 Use `browse_s3_logs(prefix='{log_uri.replace('s3://' + EMR_LOG_BUCKET + '/', '')}')` to browse")

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
    # If read_both, get stdout first then stderr (errors only)
    if read_both:
        stdout_result = read_spark_driver_log(
            application_id=application_id,
            job_run_id=job_run_id,
            log_type="stdout",
            process_name=process_name,
            tail_lines=tail_lines,
            search_text=search_text,
            bucket=bucket,
        )
        stderr_result = read_spark_driver_log(
            application_id=application_id,
            job_run_id=job_run_id,
            log_type="stderr",
            process_name=process_name,
            tail_lines=min(tail_lines, 100),
            search_text="ERROR",
            bucket=bucket,
        )
        return (
            "═══ STDOUT (Python App Output — primary log) ═══\n"
            + stdout_result
            + "\n\n═══ STDERR (Spark Framework — errors only) ═══\n"
            + stderr_result
        )

    s3 = _get_s3()
    log_bucket = bucket or EMR_LOG_BUCKET
    prefix = EMR_LOG_PREFIX.strip("/")

    # If full URI given, read directly
    if s3_log_uri:
        b, k = _parse_s3_uri(s3_log_uri)
        content = _read_s3_object(b, k)
        if content is None:
            return f"❌ Could not read: {s3_log_uri}"
        return _format_log_output(content, f"s3://{b}/{k}", log_type, tail_lines, search_text)

    # Build candidate paths based on actual structure:
    # spark-logs/{process_name}/applications/{app_id}/jobs/{job_id}/SPARK_DRIVER/{log}.gz
    candidates: list[str] = []

    if process_name:
        # With explicit process name
        candidates.extend([
            f"{prefix}/{process_name}/applications/{application_id}/jobs/{job_run_id}/SPARK_DRIVER/{log_type}.gz",
            f"{prefix}/{process_name}/applications/{application_id}/jobs/{job_run_id}/SPARK_DRIVER/{log_type}",
        ])
    else:
        # Try to find by listing the process names
        try:
            list_resp = s3.list_objects_v2(
                Bucket=log_bucket,
                Prefix=f"{prefix}/",
                Delimiter="/",
            )
            common_prefixes = [p["Prefix"] for p in list_resp.get("CommonPrefixes", [])]
            for cp in common_prefixes:
                # Each cp is like "spark-logs/ttdgeo_metadata_SE/"
                candidates.append(f"{cp}applications/{application_id}/jobs/{job_run_id}/SPARK_DRIVER/{log_type}.gz")
                candidates.append(f"{cp}applications/{application_id}/jobs/{job_run_id}/SPARK_DRIVER/{log_type}")
        except Exception:
            pass

    # Also try without process_name (flat structure)
    candidates.extend([
        f"{prefix}/applications/{application_id}/jobs/{job_run_id}/SPARK_DRIVER/{log_type}.gz",
        f"{prefix}/applications/{application_id}/jobs/{job_run_id}/SPARK_DRIVER/{log_type}",
    ])

    # Try each candidate
    for key in candidates:
        content = _read_s3_object(log_bucket, key)
        if content is not None:
            return _format_log_output(content, f"s3://{log_bucket}/{key}", log_type, tail_lines, search_text)

    # Not found — try listing to help the user
    return _find_log_suggestions(log_bucket, prefix, application_id, job_run_id, log_type)


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


def _find_log_suggestions(
    bucket: str,
    prefix: str,
    app_id: str,
    job_id: str,
    log_type: str,
) -> str:
    """When log not found, list available files to help the user."""
    s3 = _get_s3()
    lines = [f"❌ Spark driver `{log_type}` log not found.\n"]

    # Try listing under app_id
    search_patterns = [
        f"{prefix}/",
    ]

    for search_prefix in search_patterns:
        try:
            paginator = s3.get_paginator("list_objects_v2")
            found_any = False
            for resp in paginator.paginate(Bucket=bucket, Prefix=search_prefix, Delimiter="/"):
                for cp in resp.get("CommonPrefixes", []):
                    subdir = cp["Prefix"]
                    # Check if this subdir has our app_id somewhere inside
                    try:
                        sub_check = s3.list_objects_v2(
                            Bucket=bucket,
                            Prefix=f"{subdir}applications/{app_id}/",
                            MaxKeys=5,
                        )
                        if sub_check.get("Contents"):
                            found_any = True
                            files = [c["Key"] for c in sub_check["Contents"]]
                            lines.append(f"📂 Found logs under: `{subdir}`")
                            for f in files:
                                lines.append(f"   • `{f}`")
                            process = subdir.replace(f"{prefix}/", "").strip("/")
                            lines.append(
                                f"   💡 Try: `read_spark_driver_log(application_id='{app_id}', "
                                f"job_run_id='{job_id}', process_name='{process}')`"
                            )
                            lines.append("")
                    except Exception:
                        pass
            if found_any:
                return "\n".join(lines)
        except Exception as exc:
            lines.append(f"   ⚠️  Could not list bucket: {exc}")

    lines.append(f"💡 Use `browse_s3_logs()` to explore the log structure in `s3://{bucket}/{prefix}/`")
    return "\n".join(lines)


def browse_s3_logs(
    prefix: str | None = None,
    bucket: str | None = None,
    max_items: int = 100,
) -> str:
    """
    Browse the S3 log directory structure. Navigate into folders to find logs.

    Args:
        prefix: S3 prefix/path to browse (default: EMR_LOG_PREFIX from config, e.g. 'spark-logs/').
                Use the output to navigate deeper, e.g. 'spark-logs/ttdgeo_metadata_SE/'.
        bucket: S3 bucket (default from config).
        max_items: Max items to show (default 50).

    Returns a directory listing of the S3 prefix showing folders and files.
    """
    s3 = _get_s3()
    log_bucket = bucket or EMR_LOG_BUCKET
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
        return f"📂 Empty: `s3://{log_bucket}/{log_prefix}` — no files or folders found."

    lines = [f"📂 **Browsing: `s3://{log_bucket}/{log_prefix}`**\n"]

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


def cancel_job_run(application_id: str, job_run_id: str) -> str:
    """
    Cancel a running or pending EMR Serverless job run.

    Use this when a Spark job is stuck, taking too long, or was started
    with incorrect parameters. The cancellation is asynchronous — the job
    will transition to CANCELLING and then CANCELLED state.

    Args:
        application_id: The EMR Serverless application ID.
        job_run_id: The job run ID to cancel.

    Returns confirmation of the cancellation request.
    """
    client = _get_emr()

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
) -> str:
    """
    Read any file from S3 by its full URI.

    Use this for reading input data files, output files, configuration files,
    or any S3 object — not just Spark logs. Auto-decompresses .gz files.

    Args:
        s3_uri: Full S3 URI (e.g. 's3://bucket-name/path/to/file.csv').
        tail_lines: Number of lines from the end to return (default 100). Set to -1 for all.
        search_text: Optional text to filter matching lines.

    Returns the file contents, optionally filtered and tailed.
    """
    if not s3_uri.startswith("s3://"):
        return f"❌ Invalid S3 URI: '{s3_uri}'. Must start with 's3://'."

    bucket, key = _parse_s3_uri(s3_uri)
    if not key:
        return f"❌ No key/path specified in URI: '{s3_uri}'."

    content = _read_s3_object(bucket, key)
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
) -> str:
    """
    Get a summary of EMR Serverless resource usage and estimated costs.

    Aggregates vCPU hours, memory GB-hours, and storage GB-hours across
    recent job runs. Useful for understanding compute costs.

    Args:
        application_id: Optional — filter to one application. If omitted, scans all.
        days: Number of days to look back (default 7).

    Returns a cost summary with per-job and total resource usage.
    """
    client = _get_emr()
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
