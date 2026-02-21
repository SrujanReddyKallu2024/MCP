"""
MWAA / Airflow tools.

Uses create_web_login_token() to authenticate, then calls the Airflow REST API
directly via the MWAA web server endpoint.  This works for both public and
**private** MWAA environments (when VPN provides network access to the endpoint).

Tools:
  • list_dags            — list all DAGs with schedule & pause status
  • get_dag_run_history         — DAG runs for today/yesterday/date with pass/fail
  • get_dag_run_details  — full task-level breakdown for one DAG run
  • get_task_log         — read the Airflow log of a single task attempt
  • trigger_dag          — manually trigger a DAG run
  • pause_dag            — pause a DAG to prevent scheduled runs
  • unpause_dag          — unpause a DAG to resume scheduling
  • clear_task_instance  — retry a failed task without re-triggering the DAG
  • get_dag_source       — get DAG source code, tasks, and metadata
  • get_dag_status_report — full dashboard of ALL DAGs with last run status
  • get_dag_run_stats   — run statistics with trends, success rate, duration stats
"""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Any

import boto3
import requests
import urllib3

from mcp_server.config import (
    AWS_REGION,
    get_aws_profile,
    MWAA_ENVIRONMENTS,
    get_mwaa_env_name,
    validate_mwaa,
)
from mcp_server.tools._aws_helpers import fmt_duration

# Suppress SSL warnings for internal/self-signed certs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ── Airflow session cache (per-environment) ──────────────────────────────────

_sessions: dict[str, tuple[requests.Session, str]] = {}   # env_name → (session, hostname)


def _get_airflow_session(env_name: str, env: str | None = None) -> tuple[requests.Session, str]:
    """
    Create an authenticated requests.Session for the MWAA Airflow REST API.

    Strategy:
      1. Try create_web_login_token + POST to /aws_mwaa/login (standard flow)
      2. Fall back to create_cli_token + Bearer header (works for private VPCE)
    """
    if env_name in _sessions:
        return _sessions[env_name]

    # Fresh boto3 session — uses the env-specific AWS profile (different accounts)
    boto_session = boto3.Session(region_name=AWS_REGION, profile_name=get_aws_profile(env))
    mwaa_client = boto_session.client("mwaa")

    session = requests.Session()
    session.verify = False  # Internal/self-signed certs

    # ── Strategy 1: Web login token (POST form data) ──
    try:
        token_resp = mwaa_client.create_web_login_token(Name=env_name)
        hostname = token_resp["WebServerHostname"]
        web_token = token_resp["WebToken"]

        login_url = f"https://{hostname}/aws_mwaa/login"
        login_resp = session.post(
            login_url,
            data={"token": web_token},
            allow_redirects=True,
            timeout=30,
        )

        if login_resp.status_code == 200 and session.cookies.get("session"):
            _sessions[env_name] = (session, hostname)
            return session, hostname
    except Exception:
        pass  # Fall through to strategy 2

    # ── Strategy 2: CLI token as Bearer auth ──
    try:
        cli_resp = mwaa_client.create_cli_token(Name=env_name)
        hostname = cli_resp["WebServerHostname"]
        cli_token = cli_resp["CliToken"]

        session.headers.update({
            "Authorization": f"Bearer {cli_token}",
            "Content-Type": "application/json",
        })

        _sessions[env_name] = (session, hostname)
        return session, hostname

    except Exception as exc:
        raise RuntimeError(
            f"Cannot authenticate to MWAA '{env_name}'. "
            f"Both web login token and CLI token failed. Error: {exc}"
        ) from exc


def _clear_session(env_name: str):
    """Clear cached session for an environment (e.g. after auth failure)."""
    _sessions.pop(env_name, None)


# ── Helper: call Airflow REST API via MWAA ───────────────────────────────────

def _airflow_api(
    method: str,
    path: str,
    env: str | None = None,
    body: dict | None = None,
    query_params: dict | None = None,
) -> dict[str, Any]:
    """
    Call the Airflow REST API through the MWAA web server.
    Uses create_web_login_token for authentication.
    Returns the parsed JSON response body.
    """
    err = validate_mwaa(env)
    if err:
        return {"error": err}

    env_name = get_mwaa_env_name(env)
    verb = method.upper()

    def _do_request(sess: requests.Session, host: str) -> requests.Response:
        url = f"https://{host}{path}"
        kwargs: dict[str, Any] = {"params": query_params, "timeout": 30}
        if verb in ("POST", "PATCH", "PUT"):
            kwargs["json"] = body
        return sess.request(verb, url, **kwargs)

    try:
        session, hostname = _get_airflow_session(env_name, env=env)
        resp = _do_request(session, hostname)

        # Token may have expired — clear session and retry once
        if resp.status_code in (401, 403):
            _clear_session(env_name)
            session, hostname = _get_airflow_session(env_name, env=env)
            resp = _do_request(session, hostname)

        if resp.status_code >= 400:
            return {"error": f"Airflow API returned HTTP {resp.status_code}: {resp.text[:500]}"}

        # Log endpoint may return plain text
        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type:
            return resp.json()
        else:
            return {"content": resp.text}

    except requests.exceptions.ConnectionError as exc:
        _clear_session(env_name)
        return {"error": f"Cannot connect to MWAA webserver. Is VPN connected? Error: {exc}"}
    except Exception as exc:
        _clear_session(env_name)
        return {"error": f"MWAA API call failed: {exc}"}



# ── Formatting helpers ───────────────────────────────────────────────────────

_STATUS_EMOJI = {
    "success":  "✅",
    "failed":   "❌",
    "running":  "⏳",
    "queued":   "🟡",
    "upstream_failed": "⛔",
    "skipped":  "⏭️",
    "up_for_retry": "🔄",
    "up_for_reschedule": "🔄",
    "deferred": "⏸️",
    "removed":  "🗑️",
    "no_status": "⚪",
    "scheduled": "📅",
    "restarting": "🔁",
}


def _emoji(state: str | None) -> str:
    return _STATUS_EMOJI.get((state or "").lower(), "❓")


_fmt_duration = fmt_duration


def _env_header(env: str | None) -> str:
    """Friendly header showing which environment we're talking to."""
    tag = (env or "dev").upper()
    name = get_mwaa_env_name(env) or "?"
    available = ", ".join(k.upper() for k in MWAA_ENVIRONMENTS.keys())
    return f"🌍 Environment: **{tag}** (`{name}`)  |  Available: {available}\n"


# ═══════════════════════════════════════════════════════════════════════════════
# TOOLS
# ═══════════════════════════════════════════════════════════════════════════════


def list_dags(
    env: str | None = None,
    limit: int = 100,
    only_active: bool = True,
) -> str:
    """
    List all DAGs in the MWAA environment.

    Args:
        env: Which environment — 'dev', 'uat', 'test', or 'prod' (default: dev).
        limit: Maximum number of DAGs to return (default 100).
        only_active: If True, show only unpaused DAGs.

    Returns a formatted table of DAGs with schedule interval and pause status.
    """
    params = {"limit": str(limit)}
    if only_active:
        params["only_active"] = "true"

    data = _airflow_api("GET", "/api/v1/dags", env=env, query_params=params)
    if "error" in data:
        return data["error"]

    dags = data.get("dags", [])
    if not dags:
        return _env_header(env) + "No DAGs found."

    lines = [_env_header(env), f"📋 **{len(dags)} DAGs found**\n"]
    lines.append(f"{'DAG ID':<55} {'Schedule':<25} {'Status'}")
    lines.append("─" * 95)

    for d in sorted(dags, key=lambda x: x.get("dag_id", "")):
        dag_id = d.get("dag_id", "?")
        sched = d.get("schedule_interval", {})
        if isinstance(sched, dict):
            sched_str = sched.get("value", "None")
        else:
            sched_str = str(sched) if sched else "None"
        paused = "⏸ Paused" if d.get("is_paused") else "▶ Active"
        lines.append(f"{dag_id:<55} {sched_str:<25} {paused}")

    return "\n".join(lines)


def get_dag_run_history(
    env: str | None = None,
    dag_id: str | None = None,
    date: str = "today",
    limit: int = 50,
) -> str:
    """
    Get DAG runs for today, yesterday, last week, or a specific date.

    This is the go-to tool when the user asks about DAG runs, processing
    status, or historical failures. Returns a numbered interactive list
    so the user can pick a specific run for deeper investigation.

    Args:
        env: Which environment — 'dev', 'uat', 'test', or 'prod' (default: dev).
        dag_id: Optional — filter to a specific DAG. If omitted, shows ALL DAGs.
        date: 'today' (default), 'yesterday', 'last_week', or ISO date (e.g. '2026-02-15').
        limit: Max runs to return (default 50).

    Returns a numbered list of runs with status, timing and duration.
    Present these to the user so they can pick one by number.
    """
    from datetime import timedelta

    now = datetime.now(timezone.utc)

    if date.lower() == "today":
        start_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = now + timedelta(days=1)
        label = f"today ({start_dt.strftime('%Y-%m-%d')})"
    elif date.lower() == "yesterday":
        start_dt = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = start_dt + timedelta(days=1)
        label = f"yesterday ({start_dt.strftime('%Y-%m-%d')})"
    elif date.lower() == "last_week":
        start_dt = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = now
        label = "last 7 days"
    else:
        try:
            start_dt = datetime.fromisoformat(date).replace(tzinfo=timezone.utc)
            end_dt = start_dt + timedelta(days=1)
            label = start_dt.strftime("%Y-%m-%d")
        except ValueError:
            return f"❌ Invalid date: '{date}'. Use 'today', 'yesterday', 'last_week', or ISO format (e.g. '2026-02-15')."

    body: dict[str, Any] = {
        "execution_date_gte": start_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "execution_date_lte": end_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "page_limit": limit,
        "order_by": "-execution_date",
    }
    if dag_id:
        body["dag_ids"] = [dag_id]

    data = _airflow_api("POST", "/api/v1/dags/~/dagRuns/list", env=env, body=body)
    if "error" in data:
        return data["error"]

    runs = data.get("dag_runs", [])
    if not runs:
        return _env_header(env) + f"No DAG runs found for {label}."

    # Group by state for summary
    state_counts: dict[str, int] = {}
    for run in runs:
        st = run.get("state", "unknown")
        state_counts[st] = state_counts.get(st, 0) + 1

    summary_parts = [f"{_emoji(s)} {s}: {c}" for s, c in sorted(state_counts.items())]

    lines = [
        _env_header(env),
        f"📅 **DAG Runs for {label} — {len(runs)} run(s)**",
        f"   Summary: {' | '.join(summary_parts)}",
        "",
    ]

    # Numbered list for interactive selection
    for idx, run in enumerate(runs, 1):
        state = run.get("state", "unknown")
        emoji = _emoji(state)
        rid = run.get("dag_run_id", "?")
        did = run.get("dag_id", "?")
        start = run.get("start_date")
        end = run.get("end_date")
        dur = _fmt_duration(start, end)
        start_short = start[:19] if start else "—"

        lines.append(f"{idx}. {emoji} **{did}** — {state.upper()}")
        lines.append(f"   Run ID   : {rid}")
        lines.append(f"   Started  : {start_short}  |  Duration: {dur}")
        if state == "failed":
            lines.append(f"   💡 `get_dag_run_details(dag_id='{did}', dag_run_id='{rid}', env='{env or 'dev'}')`")
        lines.append("")

    if len(runs) > 1:
        lines.append("💡 **Pick a number above** to investigate a specific run, or ask for details on a failed one.")

    return "\n".join(lines)


def get_dag_run_details(
    dag_id: str,
    dag_run_id: str,
    env: str | None = None,
) -> str:
    """
    Get full details for a specific DAG run, including every task instance.

    Use this to see ALL tasks in a DAG run with their pass/fail status.
    For any failed tasks, the output includes ready-to-use get_task_log() hints.
    Common DAG task flow: start → create_arguments → check_inputs → initialise (creates EMR app) → processing → finalise.

    Args:
        dag_id: The DAG identifier.
        dag_run_id: The run ID (e.g. 'scheduled__2026-02-16T00:00:00+00:00' or 'manual__...').
        env: Which environment — 'dev', 'uat', 'test', or 'prod' (default: dev).

    Returns formatted output showing each task with state, duration and try count.
    """
    # Fetch DAG run metadata
    run_data = _airflow_api("GET", f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}", env=env)
    if "error" in run_data:
        return run_data["error"]

    # Fetch task instances
    ti_data = _airflow_api("GET", f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances", env=env)
    if "error" in ti_data:
        return ti_data["error"]

    state = run_data.get("state", "unknown")
    emoji = _emoji(state)
    start = run_data.get("start_date")
    end = run_data.get("end_date")
    dur = _fmt_duration(start, end)
    conf = run_data.get("conf", {})

    lines = [
        _env_header(env),
        f"{emoji} **DAG Run: {dag_id}** — `{dag_run_id}`",
        f"   State    : {state.upper()}",
        f"   Started  : {(start or '—')[:19]}",
        f"   Ended    : {(end or '—')[:19]}",
        f"   Duration : {dur}",
    ]
    if conf:
        lines.append(f"   Config   : {json.dumps(conf, indent=2)[:500]}")

    lines.extend([
        "",
        "**Task Instances:**",
        f"{'Task':<45} {'State':<18} {'Duration':<12} {'Tries'}",
        "─" * 90,
    ])

    tasks = ti_data.get("task_instances", [])
    tasks.sort(key=lambda t: (t.get("map_index", 0), t.get("start_date", "") or ""))

    for t in tasks:
        tid = t.get("task_id", "?")
        tstate = t.get("state", "no_status")
        temoji = _emoji(tstate)
        tstart = t.get("start_date")
        tend = t.get("end_date")
        tdur = _fmt_duration(tstart, tend)
        tries = t.get("try_number", 0)
        lines.append(f"{temoji} {tid:<43} {tstate:<18} {tdur:<12} {tries}")

    # Summary of failures
    failed = [t for t in tasks if t.get("state") == "failed"]
    if failed:
        lines.append("")
        lines.append(f"❌ **{len(failed)} FAILED task(s):** " + ", ".join(t["task_id"] for t in failed))
        for ft in failed:
            ftid = ft["task_id"]
            tries = ft.get("try_number", 1)
            lines.append(
                f"   💡 `get_task_log(dag_id='{dag_id}', dag_run_id='{dag_run_id}', "
                f"task_id='{ftid}', try_number={tries}, env='{env or 'dev'}')`"
            )

    return "\n".join(lines)


def get_task_log(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    env: str | None = None,
    try_number: int = 1,
    tail_lines: int = 200,
) -> str:
    """
    Read the Airflow log for a specific task attempt.

    IMPORTANT for EMR debugging:
    - Read the 'initialise' (or 'ae_initialize_emr_application') task log to find the EMR application ID.
      Look for: 'EMR serverless application created: 00gXXXXXXXXX' or 'Created EMR application 00gXXXXXXXXX'.
    - Read the failed processing task log to find the job_run_id.
      Look for: 'EMR serverless job started: 00gXXXXXXXXX'.
    - Then use read_spark_driver_log(application_id, job_run_id, log_type='stdout') to get the Python app output.

    Args:
        dag_id: The DAG identifier.
        dag_run_id: The run ID.
        task_id: The task identifier.
        env: Which environment — 'dev', 'uat', 'test', or 'prod' (default: dev).
        try_number: Which attempt (default 1).
        tail_lines: Number of lines to return from the end (default 200).

    Returns the raw log text, trimmed to the last N lines.
    """
    path = f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
    data = _airflow_api("GET", path, env=env)
    if isinstance(data, dict) and "error" in data:
        return data["error"]

    # The log response may be plain text or wrapped in JSON
    if isinstance(data, str):
        log_text = data
    elif isinstance(data, dict):
        log_text = data.get("content", "") or data.get("log", "") or json.dumps(data, indent=2)
    else:
        log_text = str(data)

    # Tail the log
    all_lines = log_text.strip().splitlines()
    total = len(all_lines)
    if total > tail_lines:
        display_lines = all_lines[-tail_lines:]
        header = f"📄 **Task Log: {task_id}** (last {tail_lines} of {total} lines)\n"
    else:
        display_lines = all_lines
        header = f"📄 **Task Log: {task_id}** ({total} lines)\n"

    header += f"   DAG: {dag_id} | Run: {dag_run_id} | Try: {try_number}\n"

    return header + "\n".join(display_lines)


def trigger_dag(
    dag_id: str,
    env: str | None = None,
    conf: str | None = None,
) -> str:
    """
    Manually trigger a DAG run.

    Args:
        dag_id: The DAG to trigger.
        env: Which environment — 'dev', 'uat', 'test', or 'prod' (default: dev).
        conf: Optional JSON string of DAG run configuration.

    Returns confirmation with the new run ID.
    """
    body: dict[str, Any] = {}
    if conf:
        try:
            body["conf"] = json.loads(conf)
        except json.JSONDecodeError:
            return f"❌ Invalid JSON in conf: {conf}"

    data = _airflow_api("POST", f"/api/v1/dags/{dag_id}/dagRuns", env=env, body=body)
    if "error" in data:
        return data["error"]

    run_id = data.get("dag_run_id", "?")
    state = data.get("state", "?")
    return _env_header(env) + f"✅ Triggered **{dag_id}** — Run ID: `{run_id}` — State: {state}"



def pause_dag(dag_id: str, env: str | None = None) -> str:
    """
    Pause a DAG — prevents future scheduled runs from triggering.

    The DAG will still be visible in the Airflow UI but won't execute
    on its schedule. Already-running DAG runs will continue to completion.

    Args:
        dag_id: The DAG to pause.
        env: Which environment — 'dev', 'uat', 'test', or 'prod' (default: dev).

    Returns confirmation of the pause action.
    """
    data = _airflow_api("PATCH", f"/api/v1/dags/{dag_id}", env=env, body={"is_paused": True})
    if "error" in data:
        return data["error"]

    paused = data.get("is_paused", None)
    if paused is True:
        return _env_header(env) + f"⏸️ **{dag_id}** is now PAUSED — scheduled runs will NOT trigger."
    return _env_header(env) + f"⚠️ Unexpected response for {dag_id}: {json.dumps(data)[:300]}"


def unpause_dag(dag_id: str, env: str | None = None) -> str:
    """
    Unpause a DAG — allows scheduled runs to trigger again.

    Args:
        dag_id: The DAG to unpause.
        env: Which environment — 'dev', 'uat', 'test', or 'prod' (default: dev).

    Returns confirmation of the unpause action.
    """
    data = _airflow_api("PATCH", f"/api/v1/dags/{dag_id}", env=env, body={"is_paused": False})
    if "error" in data:
        return data["error"]

    paused = data.get("is_paused", None)
    if paused is False:
        return _env_header(env) + f"▶️ **{dag_id}** is now ACTIVE — scheduled runs will trigger."
    return _env_header(env) + f"⚠️ Unexpected response for {dag_id}: {json.dumps(data)[:300]}"


def clear_task_instance(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    env: str | None = None,
    include_downstream: bool = False,
) -> str:
    """
    Clear a task instance to retry it without re-triggering the entire DAG.

    Use this when a task failed due to a transient issue (e.g. network timeout,
    temporary S3 error) and you want to retry just that task and optionally
    all tasks downstream of it.

    Args:
        dag_id: The DAG identifier.
        dag_run_id: The run ID.
        task_id: The task to clear/retry.
        env: Which environment — 'dev', 'uat', 'test', or 'prod' (default: dev).
        include_downstream: If True, also clear all downstream tasks (default: False).

    Returns confirmation with the list of cleared task instances.
    """
    body: dict[str, Any] = {
        "dag_run_id": dag_run_id,
        "include_downstream": include_downstream,
        "include_upstream": False,
        "include_future": False,
        "include_past": False,
        "task_ids": [task_id],
        "dry_run": False,
    }

    data = _airflow_api("POST", f"/api/v1/dags/{dag_id}/clearTaskInstances", env=env, body=body)
    if "error" in data:
        return data["error"]

    cleared = data.get("task_instances", [])
    if not cleared:
        return _env_header(env) + f"⚠️ No task instances were cleared for `{task_id}`."

    lines = [
        _env_header(env),
        f"🔄 **Cleared {len(cleared)} task instance(s)** — they will retry automatically",
        "",
    ]
    for ti in cleared:
        tid = ti.get("task_id", "?")
        state = ti.get("state", "?")
        lines.append(f"   • {tid} (was: {state})")

    lines.append("")
    lines.append(f"💡 Use `get_dag_run_details(dag_id='{dag_id}', dag_run_id='{dag_run_id}', env='{env or 'dev'}')` to monitor progress")
    return "\n".join(lines)


def get_dag_source(dag_id: str, env: str | None = None) -> str:
    """
    Get the source code / definition of a DAG.

    Use this to understand what a DAG does — its tasks, operators,
    dependencies, schedule, and configuration. Useful for debugging
    or understanding a pipeline's structure.

    Args:
        dag_id: The DAG identifier.
        env: Which environment — 'dev', 'uat', 'test', or 'prod' (default: dev).

    Returns the DAG details including file location, schedule, tags, and task list.
    """
    # Get DAG metadata (includes file_token for source retrieval)
    data = _airflow_api("GET", f"/api/v1/dags/{dag_id}", env=env)
    if "error" in data:
        return data["error"]

    dag_data = data

    lines = [
        _env_header(env),
        f"📋 **DAG: {dag_id}**",
        "",
        f"   Description   : {dag_data.get('description', '—') or '—'}",
        f"   File Location : {dag_data.get('fileloc', '—')}",
        f"   Owner(s)      : {', '.join(dag_data.get('owners', ['—']))}",
        f"   Is Paused     : {'⏸ Yes' if dag_data.get('is_paused') else '▶ No'}",
        f"   Is Active     : {'✅ Yes' if dag_data.get('is_active') else '❌ No'}",
    ]

    sched = dag_data.get("schedule_interval", {})
    if isinstance(sched, dict):
        sched_str = sched.get("value", "None")
    else:
        sched_str = str(sched) if sched else "None"
    lines.append(f"   Schedule      : {sched_str}")

    tags = dag_data.get("tags", [])
    if tags:
        tag_names = [t.get("name", "?") for t in tags]
        lines.append(f"   Tags          : {', '.join(tag_names)}")

    default_args = dag_data.get("default_view", "")
    if default_args:
        lines.append(f"   Default View  : {default_args}")

    # Try to get DAG source code via the source endpoint
    file_token = dag_data.get("file_token", "")
    if file_token:
        source_data = _airflow_api("GET", f"/api/v1/dagSources/{file_token}", env=env)
        if isinstance(source_data, dict) and "content" in source_data:
            source = source_data["content"]
            lines.append("")
            lines.append("─" * 80)
            lines.append("**DAG Source Code:**")
            lines.append("```python")
            # Limit to first 200 lines
            source_lines = source.splitlines()
            if len(source_lines) > 200:
                lines.extend(source_lines[:200])
                lines.append(f"... ({len(source_lines) - 200} more lines)")
            else:
                lines.extend(source_lines)
            lines.append("```")
        elif isinstance(source_data, dict) and "error" not in source_data:
            # Some Airflow versions return plain text
            pass

    # Get task list
    tasks_data = _airflow_api("GET", f"/api/v1/dags/{dag_id}/tasks", env=env)
    if isinstance(tasks_data, dict) and "tasks" in tasks_data:
        tasks = tasks_data["tasks"]
        lines.append("")
        lines.append(f"**Tasks ({len(tasks)}):**")
        for t in tasks:
            tid = t.get("task_id", "?")
            op = t.get("operator_name", t.get("class_ref", {}).get("class_name", "?"))
            downstream = t.get("downstream_task_ids", [])
            dep_str = f" → {', '.join(downstream)}" if downstream else ""
            lines.append(f"   • {tid} ({op}){dep_str}")

    return "\n".join(lines)


def get_dag_status_report(
    env: str | None = None,
    limit: int = 100,
) -> str:
    """
    Get a complete status dashboard of ALL DAGs — the go-to tool for any overview question.

    USE THIS TOOL when the user asks:
      - "What's the status of all DAGs?"
      - "Which DAGs failed today?"
      - "Show me a DAG report / dashboard / overview"
      - "What's running right now?"
      - "Are all DAGs healthy?"

    Shows every DAG with:
      - Active/Paused state
      - Schedule interval
      - Last run: state (success/failed/running), date, and duration
      - Summary counts at the top
      - Failed DAGs highlighted at the bottom with diagnosis commands

    Args:
        env: Which environment — 'dev', 'uat', 'test', or 'prod' (default: dev).
        limit: Maximum number of DAGs to return (default 100).

    Returns a formatted status report with every DAG and its current health.
    """
    # Step 1: Get all DAGs (active + paused)
    params = {"limit": str(limit), "only_active": "false"}
    data = _airflow_api("GET", "/api/v1/dags", env=env, query_params=params)
    if "error" in data:
        return data["error"]

    dags = data.get("dags", [])
    if not dags:
        return _env_header(env) + "No DAGs found."

    # Step 2: Batch-query recent runs for ALL DAGs (last 7 days)
    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)

    runs_body = {
        "execution_date_gte": week_ago.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "execution_date_lte": now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "page_limit": 500,
        "order_by": "-execution_date",
    }
    runs_data = _airflow_api("POST", "/api/v1/dags/~/dagRuns/list", env=env, body=runs_body)

    # Build map: dag_id → most recent run
    last_run_map: dict[str, dict] = {}
    if "error" not in runs_data:
        for run in runs_data.get("dag_runs", []):
            did = run.get("dag_id", "")
            if did not in last_run_map:
                last_run_map[did] = run

    # Step 3: Format output
    lines = [_env_header(env), f"📊 **DAG Status Report — {len(dags)} DAGs**\n"]

    # Summary counts
    active_count = sum(1 for d in dags if not d.get("is_paused"))
    paused_count = sum(1 for d in dags if d.get("is_paused"))
    failed_dags = [d["dag_id"] for d in dags if last_run_map.get(d["dag_id"], {}).get("state") == "failed"]
    running_dags = [d["dag_id"] for d in dags if last_run_map.get(d["dag_id"], {}).get("state") == "running"]

    lines.append(f"   ▶ Active: {active_count}  |  ⏸ Paused: {paused_count}  |  ❌ Failed: {len(failed_dags)}  |  ⏳ Running: {len(running_dags)}")
    lines.append("")

    # Table
    lines.append(f"{'DAG ID':<50} {'Status':<10} {'Last Run':<12} {'Last State':<15} {'Duration':<10} {'Schedule'}")
    lines.append("─" * 130)

    for d in sorted(dags, key=lambda x: x.get("dag_id", "")):
        dag_id = d.get("dag_id", "?")
        paused = "⏸ Paused" if d.get("is_paused") else "▶ Active"

        sched = d.get("schedule_interval", {})
        if isinstance(sched, dict):
            sched_str = sched.get("value", "None")
        else:
            sched_str = str(sched) if sched else "None"

        last_run = last_run_map.get(dag_id)
        if last_run:
            state = last_run.get("state", "?")
            state_display = f"{_emoji(state)} {state}"
            run_date = (last_run.get("execution_date") or "")[:10]
            dur = _fmt_duration(last_run.get("start_date"), last_run.get("end_date"))
        else:
            state_display = "⚪ no runs"
            run_date = "—"
            dur = "—"

        lines.append(f"{dag_id:<50} {paused:<10} {run_date:<12} {state_display:<15} {dur:<10} {sched_str}")

    # Highlight failed DAGs at the bottom
    if failed_dags:
        lines.append("")
        lines.append(f"❌ **{len(failed_dags)} DAG(s) with FAILED last run:**")
        for fd in failed_dags:
            lines.append(f"   • {fd}")
            lines.append(f"     💡 Use: diagnose_dag_failure(dag_id='{fd}', env='{env or 'dev'}')")

    if running_dags:
        lines.append("")
        lines.append(f"⏳ **{len(running_dags)} DAG(s) currently RUNNING:**")
        for rd in running_dags:
            lines.append(f"   • {rd}")

    return "\n".join(lines)


def get_dag_run_stats(
    dag_id: str,
    env: str | None = None,
    days: int = 14,
) -> str:
    """
    Get run statistics and trend analysis for a specific DAG.

    Use this when the user asks about DAG reliability, performance trends,
    statistics, or historical patterns — "how's digital taxonomy been running?",
    "is this DAG stable?", "show me stats for HEM processing".

    Unlike get_dag_run_history (flat list of individual runs), this tool provides:
      - Success rate and failure rate over the time period
      - Duration stats: average, min, max, and trend direction
      - Failure pattern detection (e.g. "fails on Mondays")
      - Recent run streak (visual ✅/❌ sequence)
      - Day-by-day breakdown

    Args:
        dag_id: The DAG identifier to analyse.
        env: Which environment — 'dev', 'uat', 'test', or 'prod' (default: dev).
        days: Number of days to look back (default: 14, max: 90).

    Returns a formatted analytics report with trends and patterns.
    """
    days = max(1, min(days, 90))

    now = datetime.now(timezone.utc)
    start_dt = (now - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)

    body: dict[str, Any] = {
        "dag_ids": [dag_id],
        "execution_date_gte": start_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "execution_date_lte": now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "page_limit": 500,
        "order_by": "-execution_date",
    }

    data = _airflow_api("POST", "/api/v1/dags/~/dagRuns/list", env=env, body=body)
    if "error" in data:
        return data["error"]

    runs = data.get("dag_runs", [])
    if not runs:
        return _env_header(env) + f"No runs found for **{dag_id}** in the last {days} days."

    # ── Classify runs ──
    completed_runs = []       # runs with a definite end state
    durations_secs: list[float] = []
    state_counts: dict[str, int] = {}
    day_results: dict[str, list[str]] = {}   # date_str → [state, state, ...]
    weekday_failures: dict[str, int] = {}    # "Monday" → count

    for run in runs:
        state = run.get("state", "unknown")
        state_counts[state] = state_counts.get(state, 0) + 1

        start = run.get("start_date")
        end = run.get("end_date")
        exec_date = run.get("execution_date", "")

        # Day-by-day tracking
        date_str = exec_date[:10] if exec_date else "unknown"
        day_results.setdefault(date_str, []).append(state)

        # Duration calculation (only for runs that have finished)
        if start and end and state in ("success", "failed"):
            completed_runs.append(run)
            try:
                s = datetime.fromisoformat(start.replace("Z", "+00:00"))
                e = datetime.fromisoformat(end.replace("Z", "+00:00"))
                dur_secs = (e - s).total_seconds()
                if dur_secs > 0:
                    durations_secs.append(dur_secs)
            except Exception:
                pass

        # Weekday failure tracking
        if state == "failed" and exec_date:
            try:
                dt = datetime.fromisoformat(exec_date[:10])
                day_name = dt.strftime("%A")
                weekday_failures[day_name] = weekday_failures.get(day_name, 0) + 1
            except Exception:
                pass

    total = len(runs)
    success_count = state_counts.get("success", 0)
    failed_count = state_counts.get("failed", 0)
    running_count = state_counts.get("running", 0)

    success_rate = (success_count / total * 100) if total > 0 else 0
    failure_rate = (failed_count / total * 100) if total > 0 else 0

    # ── Duration stats ──
    def _secs_to_str(s: float) -> str:
        mins, secs = divmod(int(s), 60)
        hrs, mins = divmod(mins, 60)
        if hrs:
            return f"{hrs}h {mins}m"
        return f"{mins}m {secs}s"

    avg_dur = sum(durations_secs) / len(durations_secs) if durations_secs else 0
    min_dur = min(durations_secs) if durations_secs else 0
    max_dur = max(durations_secs) if durations_secs else 0

    # ── Duration trend (compare first half vs second half) ──
    trend_label = "—"
    if len(durations_secs) >= 4:
        mid = len(durations_secs) // 2
        # runs are ordered newest-first, so second half = older runs
        recent_avg = sum(durations_secs[:mid]) / mid
        older_avg = sum(durations_secs[mid:]) / (len(durations_secs) - mid)
        diff_pct = ((recent_avg - older_avg) / older_avg * 100) if older_avg > 0 else 0
        if diff_pct > 10:
            trend_label = f"📈 Increasing (+{diff_pct:.0f}% — recent avg {_secs_to_str(recent_avg)} vs older avg {_secs_to_str(older_avg)})"
        elif diff_pct < -10:
            trend_label = f"📉 Decreasing ({diff_pct:.0f}% — recent avg {_secs_to_str(recent_avg)} vs older avg {_secs_to_str(older_avg)})"
        else:
            trend_label = f"➡️ Stable (recent avg {_secs_to_str(recent_avg)} ≈ older avg {_secs_to_str(older_avg)})"

    # ── Recent run streak ──
    streak_icons = []
    for run in runs[:15]:   # last 15 runs, newest first
        st = run.get("state", "unknown")
        if st == "success":
            streak_icons.append("✅")
        elif st == "failed":
            streak_icons.append("❌")
        elif st == "running":
            streak_icons.append("⏳")
        else:
            streak_icons.append("⚪")

    # ── Failure pattern detection ──
    failure_patterns: list[str] = []
    if failed_count >= 2 and weekday_failures:
        top_day = max(weekday_failures, key=weekday_failures.get)  # type: ignore[arg-type]
        top_count = weekday_failures[top_day]
        if top_count >= 2 and top_count >= failed_count * 0.5:
            failure_patterns.append(f"⚠️ {top_count} of {failed_count} failures happened on **{top_day}**")

    # Check for consecutive failures
    consec_fail = 0
    max_consec = 0
    for run in runs:
        if run.get("state") == "failed":
            consec_fail += 1
            max_consec = max(max_consec, consec_fail)
        else:
            consec_fail = 0
    if max_consec >= 3:
        failure_patterns.append(f"⚠️ Longest failure streak: **{max_consec} consecutive failures**")

    # Check if failures are recent
    recent_runs = runs[:5] if len(runs) >= 5 else runs
    recent_fails = sum(1 for r in recent_runs if r.get("state") == "failed")
    if recent_fails >= 3:
        failure_patterns.append(f"🔴 **{recent_fails} of last {len(recent_runs)} runs failed** — needs attention")

    # ── Build output ──
    lines = [
        _env_header(env),
        f"📊 **Run History: {dag_id}** — last {days} days\n",
        f"**Overview:**",
        f"   Total runs     : {total}",
        f"   ✅ Success      : {success_count} ({success_rate:.1f}%)",
        f"   ❌ Failed       : {failed_count} ({failure_rate:.1f}%)",
    ]
    if running_count:
        lines.append(f"   ⏳ Running      : {running_count}")
    other = total - success_count - failed_count - running_count
    if other > 0:
        lines.append(f"   ⚪ Other        : {other}")

    lines.append("")
    lines.append("**Duration Stats:**")
    if durations_secs:
        lines.append(f"   Average        : {_secs_to_str(avg_dur)}")
        lines.append(f"   Fastest        : {_secs_to_str(min_dur)}")
        lines.append(f"   Slowest        : {_secs_to_str(max_dur)}")
        lines.append(f"   Trend          : {trend_label}")
    else:
        lines.append("   No completed runs with duration data.")

    lines.append("")
    lines.append(f"**Recent Runs** (newest → oldest):")
    lines.append(f"   {' '.join(streak_icons)}")

    if failure_patterns:
        lines.append("")
        lines.append("**Failure Patterns:**")
        for pattern in failure_patterns:
            lines.append(f"   {pattern}")

    # ── Day-by-day breakdown ──
    sorted_days = sorted(day_results.keys(), reverse=True)
    if sorted_days:
        lines.append("")
        lines.append("**Day-by-Day:**")
        lines.append(f"   {'Date':<14} {'Runs':<6} {'Result'}")
        lines.append("   " + "─" * 50)
        for day_str in sorted_days[:days]:
            states = day_results[day_str]
            run_count = len(states)
            icons = " ".join(_emoji(s) for s in states)
            lines.append(f"   {day_str:<14} {run_count:<6} {icons}")

    # ── Hints ──
    lines.append("")
    if failed_count > 0:
        lines.append(f"💡 To investigate failures: `get_dag_run_history(dag_id='{dag_id}', date='last_week', env='{env or 'dev'}')`")
        lines.append(f"💡 Quick diagnosis: `diagnose_dag_failure(dag_id='{dag_id}', env='{env or 'dev'}')`")
    else:
        lines.append(f"💡 For individual run details: `get_dag_run_history(dag_id='{dag_id}', date='last_week', env='{env or 'dev'}')`")

    return "\n".join(lines)
