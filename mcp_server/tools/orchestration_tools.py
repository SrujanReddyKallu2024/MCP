"""
High-level orchestration tools.

These tools chain multiple lower-level tools together to provide
one-shot answers to complex questions. They reduce the number of
tool calls the AI needs to make for common workflows.

Tools:
  • diagnose_dag_failure — full end-to-end failure diagnosis in one call
"""

from __future__ import annotations

import re
from typing import Any

from mcp_server.tools.mwaa_tools import (
    get_dag_run_details,
    get_task_log,
    _airflow_api,
    _env_header,
    _emoji,
    _fmt_duration,
)
from mcp_server.tools.emr_tools import (
    list_job_runs,
    read_spark_driver_log,
)
from mcp_server.config import get_mwaa_env_name


def diagnose_dag_failure(
    dag_id: str,
    env: str | None = None,
    date: str | None = None,
) -> str:
    """
    One-shot diagnosis of a failed DAG run.

    This tool does everything automatically:
    1. Finds the most recent failed run (today or specified date)
    2. Identifies which task(s) failed
    3. Reads the failed task logs
    4. Extracts EMR application ID from the 'initialise' task
    5. Reads the Spark driver logs (stdout) for Python app errors
    6. Returns a complete failure analysis

    This replaces the need to call 5-6 tools manually.

    Args:
        dag_id: The DAG to diagnose (e.g. 'ttdcustom_processing').
        env: Target environment — 'dev', 'uat', 'test', or 'prod'.
             IMPORTANT: Do NOT guess or default. Ask the user which environment if not specified.
        date: Optional date to check (ISO format or 'yesterday'). Default: today.

    Returns a comprehensive failure report with root cause analysis.
    """
    lines = [_env_header(env), f"🔍 **Diagnosing: {dag_id}**\n"]

    # ── Step 1: Find the failed run ──
    target_date = date or "today"

    # Query the API directly for structured data
    from datetime import datetime, timezone, timedelta

    if date and date.lower() == "yesterday":
        now = datetime.now(timezone.utc)
        start_dt = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = start_dt + timedelta(days=1)
    elif date:
        try:
            start_dt = datetime.fromisoformat(date).replace(tzinfo=timezone.utc)
            end_dt = start_dt + timedelta(days=1)
        except ValueError:
            return f"❌ Invalid date: {date}"
    else:
        now = datetime.now(timezone.utc)
        start_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = now

    body: dict[str, Any] = {
        "execution_date_gte": start_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "execution_date_lte": end_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "dag_ids": [dag_id],
        "page_limit": 10,
        "order_by": "-execution_date",
    }

    data = _airflow_api("POST", "/api/v1/dags/~/dagRuns/list", env=env, body=body)
    if "error" in data:
        return data["error"]

    runs = data.get("dag_runs", [])
    if not runs:
        lines.append(f"✅ No runs found for `{dag_id}` — nothing to diagnose.")
        return "\n".join(lines)

    # Find the most recent failed run (or the most recent run if none failed)
    failed_run = None
    for run in runs:
        if run.get("state") == "failed":
            failed_run = run
            break

    if not failed_run:
        # No failures — show the most recent run
        latest = runs[0]
        state = latest.get("state", "?")
        lines.append(f"✅ No failed runs found. Most recent run is **{state.upper()}**.")
        lines.append(f"   Run ID: `{latest.get('dag_run_id', '?')}`")
        return "\n".join(lines)

    run_id = failed_run.get("dag_run_id", "?")
    lines.append(f"❌ **Failed Run Found:** `{run_id}`")
    lines.append(f"   Started: {str(failed_run.get('start_date', '—'))[:19]}")
    lines.append("")

    # ── Step 2: Get task details ──
    ti_data = _airflow_api(
        "GET",
        f"/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances",
        env=env,
    )
    if "error" in ti_data:
        lines.append(f"⚠️ Could not fetch tasks: {ti_data['error']}")
        return "\n".join(lines)

    tasks = ti_data.get("task_instances", [])
    failed_tasks = [t for t in tasks if t.get("state") == "failed"]

    if not failed_tasks:
        lines.append("⚠️ Run is marked as failed but no individual tasks show failure.")
        return "\n".join(lines)

    lines.append(f"**Failed Task(s):** {', '.join(t['task_id'] for t in failed_tasks)}")
    lines.append("")

    # ── Step 3: Read failed task logs ──
    emr_app_id = None
    emr_job_id = None

    for ft in failed_tasks:
        task_id = ft["task_id"]
        try_num = ft.get("try_number", 1)
        lines.append(f"─── Task: **{task_id}** (try {try_num}) ───")

        log_output = get_task_log(
            dag_id=dag_id,
            dag_run_id=run_id,
            task_id=task_id,
            env=env,
            try_number=try_num,
            tail_lines=80,
        )
        lines.append(log_output)
        lines.append("")

        # Try to extract EMR job ID from this task's log
        job_patterns = [
            r"EMR serverless job started:\s*(00g\w+)",
            r"job_run_id[\"'\s:=]+(00g\w+)",
            r"Job run ID:\s*(00g\w+)",
        ]
        for pattern in job_patterns:
            match = re.search(pattern, log_output, re.IGNORECASE)
            if match:
                emr_job_id = match.group(1)
                break

    # ── Step 4: Extract EMR application ID from initialise task ──
    init_task_names = ["initialise", "ae_initialize_emr_application", "initialize"]
    for init_name in init_task_names:
        init_tasks = [t for t in tasks if t.get("task_id") == init_name]
        if init_tasks:
            init_log = get_task_log(
                dag_id=dag_id,
                dag_run_id=run_id,
                task_id=init_name,
                env=env,
                try_number=init_tasks[0].get("try_number", 1),
                tail_lines=100,
            )

            # Extract EMR app ID
            app_patterns = [
                r"EMR serverless application created:\s*(00g\w+)",
                r"Created EMR application[^\n]*(00g\w+)",
                r"application[_\s]id[\"'\s:=]+(00g\w+)",
                r"Starting application\s+(00g\w+)",
            ]
            for pattern in app_patterns:
                match = re.search(pattern, init_log, re.IGNORECASE)
                if match:
                    emr_app_id = match.group(1)
                    break

            if emr_app_id:
                lines.append(f"🔗 **EMR Application ID:** `{emr_app_id}` (from `{init_name}` task)")
                lines.append("")
                break

    # ── Step 4b: Extract S3 log base URI from task logs ──
    # The Airflow task log contains the full S3 path for the logs — extract it
    # so we can pass it directly to read_spark_driver_log (avoids any scanning).
    s3_log_base = None
    process_name = None
    all_log_text = "\n".join(lines)

    # Try to extract full S3 URI first (most reliable)
    uri_patterns = [
        r"(s3://[^\s\"']+/spark-logs/[A-Za-z0-9_-]+)",
        r"logUri['\"]?\s*[:=]\s*['\"]?(s3://[^\s\"']+)",
    ]
    for pat in uri_patterns:
        m = re.search(pat, all_log_text)
        if m:
            s3_log_base = m.group(1).rstrip("/")
            break

    # Fallback: extract just the process_name
    if not s3_log_base:
        pname_patterns = [
            r"spark-logs/([A-Za-z0-9_-]+)/applications/",
        ]
        for pat in pname_patterns:
            m = re.search(pat, all_log_text)
            if m and m.group(1) != "applications":
                process_name = m.group(1)
                break

    # ── Step 5: Read Spark driver logs ──
    if emr_app_id:
        # If we don't have a job ID yet, try listing jobs
        if not emr_job_id:
            jobs_output = list_job_runs(application_id=emr_app_id, states="FAILED", max_results=5, env=env)
            # Try to extract job ID from output
            job_match = re.search(r"Job ID\s*:\s*(00g\w+)", jobs_output)
            if job_match:
                emr_job_id = job_match.group(1)

        if emr_job_id:
            lines.append(f"🔗 **EMR Job Run ID:** `{emr_job_id}`")
            if s3_log_base:
                lines.append(f"🔗 **S3 Log Base:** `{s3_log_base}`")
            elif process_name:
                lines.append(f"🔗 **Process Name:** `{process_name}`")
            lines.append("")

            # Build the direct S3 URI if we have the base path from the task log
            stdout_uri = None
            stderr_uri = None
            if s3_log_base:
                stdout_uri = f"{s3_log_base}/applications/{emr_app_id}/jobs/{emr_job_id}/SPARK_DRIVER/stdout.gz"
                stderr_uri = f"{s3_log_base}/applications/{emr_app_id}/jobs/{emr_job_id}/SPARK_DRIVER/stderr.gz"

            # Read stdout (Python app output)
            lines.append("─── Spark Driver STDOUT (Python output) ───")
            stdout_log = read_spark_driver_log(
                application_id=emr_app_id,
                job_run_id=emr_job_id,
                log_type="stdout",
                s3_log_uri=stdout_uri,
                tail_lines=100,
                process_name=process_name,
                env=env,
            )
            lines.append(stdout_log)
            lines.append("")

            # Always read stderr too — both logs are essential for diagnosis
            lines.append("─── Spark Driver STDERR (framework logs) ───")
            stderr_log = read_spark_driver_log(
                application_id=emr_app_id,
                job_run_id=emr_job_id,
                log_type="stderr",
                s3_log_uri=stderr_uri,
                tail_lines=50,
                search_text="ERROR",
                process_name=process_name,
                env=env,
            )
            lines.append(stderr_log)
            lines.append("")
        else:
            lines.append("⚠️ Could not identify EMR job run ID from task logs.")
    else:
        lines.append("ℹ️ No EMR application ID found — this DAG may not use EMR Serverless.")

    lines.append("")
    lines.append("─" * 80)
    lines.append("**Diagnosis complete.** Review the logs above for root cause.")

    return "\n".join(lines)
