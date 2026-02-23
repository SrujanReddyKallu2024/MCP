"""
MCP Server — Ops Tools for MWAA, EMR Serverless, S3, Confluence & Azure DevOps.

Single server exposing tools across six domains:
  • Airflow (via MWAA)   — DAG listing, run details, task logs, triggering,
                           pause/unpause, task retry, historical runs, DAG source,
                           run statistics with trends and analytics
  • EMR Serverless       — app listing, job runs, Spark driver S3 logs, S3 browsing,
                           job cancellation, general S3 file reading, cost summary
  • S3 (General)         — bucket listing, interactive folder/file browsing,
                           object metadata inspection (any bucket in the account)
  • Confluence           — search, page content, child pages, attachments, labels,
                           comments, page creation, page updates
  • Azure DevOps (TFS)   — Git repos, file browsing, sprints, work items, backlog
  • Orchestration        — one-shot DAG failure diagnosis
  • Utilities            — server health check

Runs via stdio transport — designed for Gemini CLI or any MCP client.
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

# ── Create server ────────────────────────────────────────────────────────────

mcp = FastMCP(
    "ops-tools",
    instructions=(
        "This server provides operations tools for AWS MWAA (Airflow), "
        "EMR Serverless, S3, Confluence, Azure DevOps (TFS), and utility functions."
        "\n\n"
        "DOCUMENTATION: When the user says 'documentation', 'docs', 'runbook', "
        "or 'wiki', ALWAYS use Confluence tools. search_confluence to find, "
        "get_page_content to read. Do NOT ask 'where should I look?' — Confluence is the default."
        "\n\n"
        "INTERACTIVE OUTPUT: When the user asks about runs, logs, or processing "
        "for a specific DAG/process but doesn't specify which run — call list_dag_runs "
        "to get a numbered list and present it so the user can pick one. "
        "Do NOT ask multiple follow-up questions. Give the list in ONE shot. "
        "Example: 'I found 10 runs for HEM processing. Which one? 1. ✅ Feb 19 SUCCESS, "
        "2. ❌ Feb 18 FAILED, ...' — let them pick by number."
        "\n\n"
        "LOG PRIORITY: read_spark_driver_log defaults to stdout (the primary log "
        "with Python print statements, row counts, file paths, and errors). "
        "Use read_both=True to get stdout + stderr in one call. "
        "Only use log_type='stderr' when specifically investigating Spark framework issues."
        "\n\n"
        "DAG STATUS: When the user asks for a status report, dashboard, overview, "
        "'which DAGs failed', or 'what's running' — use get_dags_status_dashboard(). "
        "It shows EVERY DAG with last run state, pause status, and schedule in one view. "
        "Use list_dags() only when the user specifically wants just DAG names/schedules."
        "\n\n"
        "DAG STATS & TRENDS: When the user asks 'how has this DAG been running?', "
        "'is it stable?', 'show me stats', 'success rate', or 'run statistics' — use "
        "dag_analytics(dag_id='...', days=14). It provides success/failure rates, "
        "duration stats (avg/min/max), trend direction, failure patterns (e.g. 'fails on Mondays'), "
        "and a visual streak of recent runs. Use list_dag_runs() for a flat list of individual runs; "
        "use dag_analytics() for analytics and trends."
        "\n\n"
        "DEBUGGING WORKFLOW: For quick diagnosis, use diagnose_dag_failure(dag_id='...') "
        "which does everything automatically. For manual step-by-step: "
        "(1) list_dag_runs → find the failed run, "
        "(2) get_dag_run_details → find which task failed, "
        "(3) get_task_log on 'initialise' task → get EMR application ID (pattern: '00g...'), "
        "(4) get_task_log on failed processing task → get job_run_id AND the S3 log path, "
        "(5) read_spark_driver_log — ALWAYS pass s3_log_uri or process_name for speed. "
        "CRITICAL SPEED TIP: The Airflow task log from step (4) contains a line like "
        "'S3 logs available at: https://console.aws.amazon.com/s3/buckets/<bucket>?...&prefix=spark-logs/<process_name>/applications/<app_id>/jobs/<job_id>/' "
        "— extract the bucket and prefix to build s3_log_uri='s3://<bucket>/spark-logs/<process_name>/applications/<app_id>/jobs/<job_id>/SPARK_DRIVER/stdout.gz' "
        "and pass it directly. This reads in ~1 second vs minutes without it. "
        "Alternatively, extract just the process_name (e.g. 'digital_taxonomy_a16_stats') from "
        "'spark-logs/<process_name>/applications/...' and pass it as process_name parameter."
        "\n\n"
        "DAG LIFECYCLE: Use pause_dag/unpause_dag to control scheduling. "
        "Use clear_task_instance to retry a failed task without re-triggering the whole DAG."
        "\n\n"
        "S3 BROWSING: For general S3 questions (buckets, folders, files in any bucket), "
        "use list_s3_buckets() to see all buckets, browse_s3(bucket='...') to navigate "
        "folders and files interactively, get_s3_object_info(s3_uri='...') for file metadata. "
        "Use read_s3_file(s3_uri='...') to read file contents. "
        "Use list_s3_recursive(bucket='...') when the user wants to see ALL files end-to-end, "
        "recursively, in a single call — supports name/extension filters and gives size summary. "
        "For EMR Spark log navigation specifically, use browse_s3_logs and read_spark_driver_log."
        "\n\n"
        "ENVIRONMENT SELECTION — MANDATORY: All AWS tools (MWAA, EMR, S3) require the 'env' "
        "parameter: 'dev', 'uat', 'test', or 'prod'. Each env is a DIFFERENT AWS account. "
        "CRITICAL RULE: If the user does NOT explicitly say which environment in their message, "
        "you MUST ask 'Which environment? dev, uat, test, or prod?' BEFORE calling ANY AWS tool. "
        "NEVER call a tool without env. NEVER default to dev silently. NEVER call the same tool "
        "on multiple environments unless the user explicitly asked for all environments. "
        "If the user said 'dev' earlier in conversation, you may reuse that — but if it's a new "
        "topic or unclear, ASK again. When in doubt, always ask."
        "\n\n"
        "AZURE DEVOPS: When the user asks about repos, source code, sprints, "
        "work items, PBIs, tasks, bugs, backlogs, or iterations — use Azure DevOps tools. "
        "list_repos to see repos, browse_repo to explore one folder at a time, "
        "browse_repo_recursive to see the ENTIRE repo file tree in one call (use this when "
        "the user asks 'what files are in this repo?', 'show me the structure', or 'list all files'). "
        "read_repo_file to read a file's content. "
        "get_current_sprint for active sprint, get_sprint_work_items for sprint content, "
        "get_work_item_details for full PBI/Task/Bug details, get_backlog for backlog."
    ),
)


# ── Import tool functions ────────────────────────────────────────────────────

from mcp_server.tools.utility_tools import (          # noqa: E402
    server_health_check,
)
from mcp_server.tools.mwaa_tools import (             # noqa: E402
    list_dags,
    list_dag_runs,
    get_dag_run_details,
    get_task_log,
    trigger_dag,
    pause_dag,
    unpause_dag,
    clear_task_instance,
    get_dag_source,
    get_dags_status_dashboard,
    dag_analytics,
)
from mcp_server.tools.emr_tools import (              # noqa: E402
    list_emr_applications,
    list_job_runs,
    get_job_run_details,
    read_spark_driver_log,
    browse_s3_logs,
    cancel_job_run,
    stop_emr_application,
    read_s3_file,
    get_emr_cost_summary,
)
from mcp_server.tools.s3_tools import (               # noqa: E402
    list_s3_buckets,
    browse_s3,
    get_s3_object_info,
    list_s3_recursive,
)
from mcp_server.tools.confluence_tools import (       # noqa: E402
    search_confluence,
    get_page_content,
    get_child_pages,
    get_space_pages,
    get_page_attachments,
    get_page_labels,
    get_page_comments,
    create_confluence_page,
    update_confluence_page,
)
from mcp_server.tools.azdo_tools import (              # noqa: E402
    list_repos,
    browse_repo,
    browse_repo_recursive,
    read_repo_file,
    get_current_sprint,
    get_sprint_work_items,
    get_work_item_details,
    get_backlog,
)
from mcp_server.tools.orchestration_tools import (    # noqa: E402
    diagnose_dag_failure,
)

# ── Register Utility tools ──────────────────────────────────────────────────

mcp.tool()(server_health_check)

# ── Register MWAA tools (11) ────────────────────────────────────────────────

mcp.tool()(list_dags)
mcp.tool()(list_dag_runs)
mcp.tool()(get_dag_run_details)
mcp.tool()(get_task_log)
mcp.tool()(trigger_dag)
mcp.tool()(pause_dag)
mcp.tool()(unpause_dag)
mcp.tool()(clear_task_instance)
mcp.tool()(get_dag_source)
mcp.tool()(get_dags_status_dashboard)
mcp.tool()(dag_analytics)

# ── Register EMR Serverless tools (9) ────────────────────────────────────────

mcp.tool()(list_emr_applications)
mcp.tool()(list_job_runs)
mcp.tool()(get_job_run_details)
mcp.tool()(read_spark_driver_log)
mcp.tool()(browse_s3_logs)
mcp.tool()(cancel_job_run)
mcp.tool()(stop_emr_application)
mcp.tool()(read_s3_file)
mcp.tool()(get_emr_cost_summary)

# ── Register S3 tools (4) ────────────────────────────────────────────────────

mcp.tool()(list_s3_buckets)
mcp.tool()(browse_s3)
mcp.tool()(get_s3_object_info)
mcp.tool()(list_s3_recursive)

# ── Register Confluence tools (9) ────────────────────────────────────────────

mcp.tool()(search_confluence)
mcp.tool()(get_page_content)
mcp.tool()(get_child_pages)
mcp.tool()(get_space_pages)
mcp.tool()(get_page_attachments)
mcp.tool()(get_page_labels)
mcp.tool()(get_page_comments)
mcp.tool()(create_confluence_page)
mcp.tool()(update_confluence_page)

# ── Register Azure DevOps tools (8) ─────────────────────────────────────────

mcp.tool()(list_repos)
mcp.tool()(browse_repo)
mcp.tool()(browse_repo_recursive)
mcp.tool()(read_repo_file)
mcp.tool()(get_current_sprint)
mcp.tool()(get_sprint_work_items)
mcp.tool()(get_work_item_details)
mcp.tool()(get_backlog)

# ── Register Orchestration tools (1) ────────────────────────────────────────

mcp.tool()(diagnose_dag_failure)


# ── Entry point ──────────────────────────────────────────────────────────────

def _log_config_warnings() -> None:
    """Print configuration warnings to stderr at startup (doesn't block server)."""
    import sys
    from mcp_server.config import (
        AWS_PROFILES, MWAA_ENVIRONMENTS, EMR_LOG_BUCKETS,
        CONFLUENCE_PAT, AZDO_PAT,
    )

    warnings = []
    if not AWS_PROFILES:
        warnings.append("No AWS profiles configured — AWS tools will not work")
    if not MWAA_ENVIRONMENTS:
        warnings.append("No MWAA environments configured — Airflow tools will not work")
    if not EMR_LOG_BUCKETS:
        warnings.append("No EMR log buckets configured — Spark log tools will not work")
    if not CONFLUENCE_PAT:
        warnings.append("CONFLUENCE_PAT not set — Confluence tools will not work")
    if not AZDO_PAT:
        warnings.append("AZDO_PAT not set — Azure DevOps tools will not work")

    if warnings:
        print("[ops-tools] Config warnings:", file=sys.stderr)
        for w in warnings:
            print(f"  - {w}", file=sys.stderr)


def main():
    """Run the MCP server via stdio transport."""
    _log_config_warnings()
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
